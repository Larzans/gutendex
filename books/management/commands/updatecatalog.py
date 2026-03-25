from subprocess import call
import json
import os
import shutil
from time import strftime, time
import sys
import urllib.request

from django.conf import settings
from django.core.mail import send_mail
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from books import utils
from books.models import *


TEMP_PATH = settings.CATALOG_TEMP_DIR

URL = 'https://gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2'
DOWNLOAD_PATH = os.path.join(TEMP_PATH, 'catalog.tar.bz2')

MOVE_SOURCE_PATH = os.path.join(TEMP_PATH, 'cache/epub')
MOVE_TARGET_PATH = settings.CATALOG_RDF_DIR

LOG_DIRECTORY = settings.CATALOG_LOG_DIR
LOG_FILE_NAME = strftime('%Y-%m-%d_%H%M%S') + '.txt'
LOG_PATH = os.path.join(LOG_DIRECTORY, LOG_FILE_NAME)

CACHE_PATH = os.path.join(os.path.dirname(settings.CATALOG_RDF_DIR), 'rdf_stat_cache.json')


# This gives a set of the names of the subdirectories in the given file path.
def get_directory_set(path):
    directory_set = set()
    for directory_item in os.listdir(path):
        item_path = os.path.join(path, directory_item)
        if os.path.isdir(item_path):
            directory_set.add(directory_item)
    return directory_set


def log(*args):
    print(*args)
    if not os.path.exists(LOG_DIRECTORY):
        os.makedirs(LOG_DIRECTORY)
    with open(LOG_PATH, 'a') as log_file:
        text = ' '.join(args) + '\n'
        log_file.write(text)


def load_stat_cache():
    if not os.path.exists(CACHE_PATH):
        return {}
    try:
        with open(CACHE_PATH, 'r') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError('Cache root must be a JSON object')
        return data
    except Exception as e:
        log('  Warning: stat cache unreadable (%s); starting fresh.' % e)
        return {}


def save_stat_cache(cache, quiet=False):
    tmp = CACHE_PATH + '.tmp'
    try:
        with open(tmp, 'w') as f:
            json.dump(cache, f, separators=(',', ':'))
        os.replace(tmp, CACHE_PATH)
        if not quiet:
            size_kb = os.path.getsize(CACHE_PATH) / 1024
            log('  Stat cache saved: %s (%.1f KB)' % (CACHE_PATH, size_kb))
    except Exception as e:
        log('  Warning: stat cache could not be saved (%s).' % e)
        try:
            os.remove(tmp)
        except OSError:
            pass


def _fmt_duration(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return '%ds' % seconds
    return '%dm %ds' % (seconds // 60, seconds % 60)


def _set_m2m_if_changed(m2m_manager, new_objects, is_new):
    """Update an M2M relation only when the set of related objects has changed.

    For new books every relation is new — just add without comparing.
    For existing books, compare current PKs (from the prefetch cache) against
    the incoming PKs; skip the DELETE+INSERT entirely when they are equal.
    Handles the empty-list case: clears all relations if new_objects is empty
    and the current set is non-empty.
    """
    if is_new:
        if new_objects:
            m2m_manager.set(new_objects, clear=False)
        return
    new_pks = {obj.pk for obj in new_objects}
    current_pks = {obj.pk for obj in m2m_manager.all()}  # uses prefetch cache
    if new_pks != current_pks:
        m2m_manager.set(new_objects, clear=True)


def put_catalog_in_db(stat_cache, limit=None):
    book_ids = []
    for directory_item in os.listdir(settings.CATALOG_RDF_DIR):
        item_path = os.path.join(settings.CATALOG_RDF_DIR, directory_item)
        if os.path.isdir(item_path):
            try:
                book_id = int(directory_item)
            except ValueError:
                # Ignore the item if it's not a book ID number.
                pass
            else:
                book_ids.append(book_id)
    book_ids.sort()
    if limit is not None:
        book_ids = book_ids[:limit]
    book_directories = [str(id) for id in book_ids]

    # DB diagnostics — logged before the loop so problems are visible up front.
    db_books      = Book.objects.count()
    db_persons    = Person.objects.count()
    db_bookshelves = Bookshelf.objects.count()
    db_languages  = Language.objects.count()
    db_subjects   = Subject.objects.count()
    db_formats    = Format.objects.count()
    db_summaries  = Summary.objects.count()
    log('  DB before run:  books=%d  persons=%d  bookshelves=%d  '
        'languages=%d  subjects=%d  formats=%d  summaries=%d' % (
            db_books, db_persons, db_bookshelves,
            db_languages, db_subjects, db_formats, db_summaries))
    log('  RDF files to scan: %d' % len(book_directories))

    # Defer WAL flushes for this session — commits remain atomic but Postgres
    # won't fsync on every commit, giving a significant write speedup.
    # Worst case on a crash: last ~200ms of commits are lost, but the stat
    # cache checkpoint handles recovery.
    with connection.cursor() as cur:
        cur.execute("SET synchronous_commit = off")

    # Pre-load small lookup tables (bookshelves, languages) into memory.
    # Subjects and persons use lazy caches — built during the run to avoid
    # loading large tables upfront while still skipping repeated DB hits.
    bookshelf_cache = {b.name: b for b in Bookshelf.objects.all()}
    language_cache  = {l.code: l for l in Language.objects.all()}
    subject_cache   = {s.name: s for s in Subject.objects.all()}
    person_cache    = {}  # keyed by gutenberg_id (int)
    log('  Caches loaded:  bookshelves=%d  languages=%d  subjects=%d' % (
        len(bookshelf_cache), len(language_cache), len(subject_cache)))

    skipped = 0
    processed = 0
    total_start = time()
    batch_start = time()

    _BOOK_UPDATE_FIELDS = [
        'copyright', 'download_count', 'media_type', 'title',
        'published_year', 'wikipedia_url', 'reading_score',
        'reading_score_value', 'related_books',
    ]
    _BATCH_SIZE = 200

    # Collect books to process in batches so we can bulk-fetch existing Book
    # rows with one SELECT per batch instead of one per book.
    pending = []   # list of (directory, id, book_path, st)

    def flush_pending():
        nonlocal processed, batch_start

        if not pending:
            return

        # ── Batch SELECTs ─────────────────────────────────────────────────────
        # Load books with M2M prefetch so phase-2 PK comparisons use the cache
        # instead of hitting the DB per book.  Formats and summaries are loaded
        # separately (they are FK relations, not M2M through-tables).
        batch_ids = [p[1] for p in pending]

        books_in_db = {
            b.gutenberg_id: b
            for b in Book.objects.filter(gutenberg_id__in=batch_ids)
                .prefetch_related('authors', 'editors', 'translators',
                                  'bookshelves', 'languages', 'subjects')
        }
        existing_book_ids = list(books_in_db.keys())
        batch_formats = {}
        batch_summaries = {}
        if existing_book_ids:
            for f in Format.objects.filter(book__gutenberg_id__in=existing_book_ids):
                batch_formats.setdefault(f.book_id, []).append(f)
            for s in Summary.objects.filter(book__gutenberg_id__in=existing_book_ids):
                batch_summaries.setdefault(s.book_id, []).append(s)

        # ── Phase 1: parse RDF files; prepare bulk write lists ────────────────
        # Assign new field values to existing Book objects (for bulk_update) and
        # build unsaved Book objects for new entries (for bulk_create).
        # Parsing happens here — outside any transaction — to keep transactions
        # as short as possible.
        book_records    = []  # (directory, id, st, book_dict, is_new)
        books_to_update = []  # existing Book objects with updated field values
        books_to_create = []  # unsaved Book objects for new entries

        for directory, id, book_path, st in pending:
            book = utils.get_book(id, book_path)
            related_books_str = ','.join(str(i) for i in book['related_books'])
            book_in_db = books_in_db.get(id)
            is_new = book_in_db is None

            if not is_new:
                book_in_db.copyright           = book['copyright']
                book_in_db.download_count       = book['downloads']
                book_in_db.media_type           = book['type']
                book_in_db.title                = book['title']
                book_in_db.published_year       = book['published_year']
                book_in_db.wikipedia_url        = book['wikipedia_url']
                book_in_db.reading_score        = book['reading_score']
                book_in_db.reading_score_value  = book['reading_score_value']
                book_in_db.related_books        = related_books_str
                books_to_update.append(book_in_db)
            else:
                books_to_create.append(Book(
                    gutenberg_id=id,
                    copyright=book['copyright'],
                    download_count=book['downloads'],
                    media_type=book['type'],
                    title=book['title'],
                    published_year=book['published_year'],
                    wikipedia_url=book['wikipedia_url'],
                    reading_score=book['reading_score'],
                    reading_score_value=book['reading_score_value'],
                    related_books=related_books_str,
                ))

            book_records.append((directory, id, st, book, is_new))

        # One UPDATE for all changed existing books; one INSERT for all new books.
        if books_to_update:
            Book.objects.bulk_update(books_to_update, _BOOK_UPDATE_FIELDS)
        if books_to_create:
            for b in Book.objects.bulk_create(books_to_create):
                books_in_db[b.gutenberg_id] = b

        # ── Phase 2: per-book M2M, formats, summaries ─────────────────────────
        for directory, id, st, book, is_new in book_records:
            book_in_db = books_in_db[id]

            try:
                with transaction.atomic():
                    ''' Make/update the authors. '''
                    _set_m2m_if_changed(
                        book_in_db.authors,
                        [get_or_create_person(a, person_cache) for a in book['authors']],
                        is_new,
                    )

                    ''' Make/update the editors. '''
                    _set_m2m_if_changed(
                        book_in_db.editors,
                        [get_or_create_person(e, person_cache) for e in book['editors']],
                        is_new,
                    )

                    ''' Make/update the translators. '''
                    _set_m2m_if_changed(
                        book_in_db.translators,
                        [get_or_create_person(t, person_cache) for t in book['translators']],
                        is_new,
                    )

                    ''' Make/update the book shelves. '''
                    bookshelves = []
                    for shelf in book['bookshelves']:
                        if shelf not in bookshelf_cache:
                            bookshelf_cache[shelf] = Bookshelf.objects.create(name=shelf)
                        bookshelves.append(bookshelf_cache[shelf])
                    _set_m2m_if_changed(book_in_db.bookshelves, bookshelves, is_new)

                    ''' Make/update the formats. '''
                    if is_new:
                        Format.objects.bulk_create([
                            Format(book=book_in_db, mime_type=mt, url=url)
                            for mt, url in book['formats'].items()
                        ])
                    else:
                        existing_formats = {
                            (f.mime_type, f.url): f
                            for f in batch_formats.get(book_in_db.pk, [])
                        }
                        keep_ids = set()
                        to_create = []
                        for mime_type, url in book['formats'].items():
                            key = (mime_type, url)
                            if key in existing_formats:
                                keep_ids.add(existing_formats[key].id)
                            else:
                                to_create.append(Format(book=book_in_db, mime_type=mime_type, url=url))
                        if to_create:
                            Format.objects.bulk_create(to_create)
                        stale_ids = {f.id for f in existing_formats.values()} - keep_ids
                        if stale_ids:
                            Format.objects.filter(id__in=stale_ids).delete()

                    ''' Make/update the languages. '''
                    languages = []
                    for language in book['languages']:
                        if language not in language_cache:
                            language_cache[language] = Language.objects.create(code=language)
                        languages.append(language_cache[language])
                    _set_m2m_if_changed(book_in_db.languages, languages, is_new)

                    ''' Make/update subjects. '''
                    subjects = []
                    for subject in book['subjects']:
                        if subject not in subject_cache:
                            subject_cache[subject] = Subject.objects.create(name=subject)
                        subjects.append(subject_cache[subject])
                    _set_m2m_if_changed(book_in_db.subjects, subjects, is_new)

                    ''' Make/update summaries. '''
                    if is_new:
                        Summary.objects.bulk_create([
                            Summary(book=book_in_db, text=text)
                            for text in book['summaries']
                        ])
                    else:
                        existing_summaries = {
                            s.text: s for s in batch_summaries.get(book_in_db.pk, [])
                        }
                        new_texts = set(book['summaries'])
                        to_create = [
                            Summary(book=book_in_db, text=text)
                            for text in new_texts
                            if text not in existing_summaries
                        ]
                        if to_create:
                            Summary.objects.bulk_create(to_create)
                        stale_ids = {
                            s.id for text, s in existing_summaries.items()
                            if text not in new_texts
                        }
                        if stale_ids:
                            Summary.objects.filter(id__in=stale_ids).delete()

            except Exception as error:
                book_json = json.dumps(book, indent=4)
                log(
                    '  Error while putting this book info in the database:\n',
                    book_json,
                    '\n'
                )
                raise error

            stat_cache[directory] = [st.st_mtime_ns, st.st_size]
            processed += 1

            if processed % 500 == 0:
                now = strftime('%H:%M:%S')
                elapsed = time() - batch_start
                log('    %s  [processed=%d  id=%d]  skipped=%d  (%.1fs)' % (
                    now, processed, id, skipped, elapsed))
                batch_start = time()
                save_stat_cache(stat_cache, quiet=True)

        pending.clear()

    for directory in book_directories:
        id = int(directory)

        book_path = os.path.join(
            settings.CATALOG_RDF_DIR,
            directory,
            'pg' + directory + '.rdf'
        )

        st = os.stat(book_path)
        cached = stat_cache.get(directory)
        if cached and cached[0] == st.st_mtime_ns and cached[1] == st.st_size:
            skipped += 1
            continue

        pending.append((directory, id, book_path, st))

        if len(pending) >= _BATCH_SIZE:
            flush_pending()

    flush_pending()  # process any remaining books

    total_elapsed = _fmt_duration(time() - total_start)
    log('    Skipped (unchanged): %d  Processed: %d  Total: %s' % (
        skipped, processed, total_elapsed))

    # DB diagnostics after the run — compare with the before numbers to spot
    # unexpected growth or missing data.
    log('  DB after run:   books=%d  persons=%d  bookshelves=%d  '
        'languages=%d  subjects=%d  formats=%d  summaries=%d' % (
            Book.objects.count(), Person.objects.count(),
            Bookshelf.objects.count(), Language.objects.count(),
            Subject.objects.count(), Format.objects.count(),
            Summary.objects.count()))

    # Refresh PostgreSQL query planner statistics after a large import so that
    # subsequent API queries use accurate cost estimates.
    if processed > 0:
        log('  Running VACUUM ANALYZE on affected tables...')
        _tables = [
            'books_book', 'books_format', 'books_summary',
            'books_book_authors', 'books_book_editors', 'books_book_translators',
            'books_book_bookshelves', 'books_book_languages', 'books_book_subjects',
        ]
        prev_autocommit = connection.autocommit
        connection.set_autocommit(True)
        try:
            with connection.cursor() as cur:
                for table in _tables:
                    cur.execute('VACUUM ANALYZE %s' % table)
        finally:
            connection.set_autocommit(prev_autocommit)
        log('  VACUUM ANALYZE done.')

    return stat_cache, {str(id) for id in book_ids}


def get_or_create_person(data, cache):
    gid = data.get('gutenberg_id')
    if gid:
        if gid in cache:
            return cache[gid]
        person = Person.objects.filter(gutenberg_id=gid).first()
        if person:
            # Only UPDATE if something actually changed — person data is stable
            # across catalog refreshes so this saves a query in most cases.
            if (person.name != data['name']
                    or person.birth_year != data['birth']
                    or person.death_year != data['death']):
                Person.objects.filter(pk=person.pk).update(
                    name=data['name'],
                    birth_year=data['birth'],
                    death_year=data['death'],
                )
        else:
            person = Person.objects.create(
                gutenberg_id=gid,
                name=data['name'],
                birth_year=data['birth'],
                death_year=data['death'],
            )
        cache[gid] = person
        return person
    # Fallback: no agent ID in RDF (rare) — not cached since there's no stable key
    person, _ = Person.objects.get_or_create(
        name=data['name'],
        birth_year=data['birth'],
        death_year=data['death'],
    )
    return person


def send_log_email():
    if not (settings.ADMIN_EMAILS or settings.EMAIL_HOST_ADDRESS):
        return

    log_text = ''
    with open(LOG_PATH, 'r') as log_file:
        log_text = log_file.read()

    email_html = '''
        <h1 style="color: #333;
                   font-family: 'Helvetica Neue', sans-serif;
                   font-size: 64px;
                   font-weight: 100;
                   text-align: center;">
            Gutendex
        </h1>

        <p style="color: #333;
                  font-family: 'Helvetica Neue', sans-serif;
                  font-size: 24px;
                  font-weight: 200;">
            Here is the log from your catalog retrieval:
        </p>

        <pre style="color:#333;
                    font-family: monospace;
                    font-size: 16px;
                    margin-left: 32px">''' + log_text + '</pre>'

    email_text = '''GUTENDEX

    Here is the log from your catalog retrieval:

    ''' + log_text

    send_mail(
        subject='Catalog retrieval',
        message=email_text,
        html_message=email_html,
        from_email=settings.EMAIL_HOST_ADDRESS,
        recipient_list=settings.ADMIN_EMAILS
    )


def prime_rdf_cache():
    if not os.path.exists(settings.CATALOG_RDF_DIR):
        log('  RDF directory does not exist; nothing to prime.')
        return
    cache = {}
    count = 0
    for directory_item in os.listdir(settings.CATALOG_RDF_DIR):
        item_path = os.path.join(settings.CATALOG_RDF_DIR, directory_item)
        if not os.path.isdir(item_path):
            continue
        try:
            int(directory_item)
        except ValueError:
            continue
        rdf_path = os.path.join(item_path, 'pg' + directory_item + '.rdf')
        try:
            st = os.stat(rdf_path)
        except OSError:
            continue
        cache[directory_item] = [st.st_mtime_ns, st.st_size]
        count += 1
    save_stat_cache(cache)
    log('  Primed cache with %d files.' % count)


class Command(BaseCommand):
    help = 'This replaces the catalog files with the latest ones.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--prime-rdf-cache',
            action='store_true',
            help=(
                'Scan the existing RDF directory, record each file\'s mtime '
                'and size into the stat cache, then exit. Use this after '
                'upgrading from a previous version of gutendex to mark all '
                'current files as already imported, so the next updatecatalog '
                'run only processes new or changed files.'
            ),
        )

    def handle(self, *args, **options):
        if options['prime_rdf_cache']:
            log('Priming RDF stat cache...')
            prime_rdf_cache()
            log('Done!')
            return

        try:
            date_and_time = strftime('%H:%M:%S on %B %d, %Y')
            log('Starting script at', date_and_time)

            log('  Making temporary directory...')
            if os.path.exists(TEMP_PATH):
                raise CommandError(
                    'The temporary path, `' + TEMP_PATH + '`, already exists.'
                )
            else:
                os.makedirs(TEMP_PATH)

            log('  Downloading compressed catalog...')
            urllib.request.urlretrieve(URL, DOWNLOAD_PATH)

            log('  Decompressing catalog...')
            if not os.path.exists(DOWNLOAD_PATH):
                os.makedirs(DOWNLOAD_PATH)
            with open(os.devnull, 'w') as null:
                call(
                    ['tar', 'fjvx', DOWNLOAD_PATH, '-C', TEMP_PATH],
                    stdout=null,
                    stderr=null
                )

            log('  Detecting stale directories...')
            if not os.path.exists(MOVE_TARGET_PATH):
                os.makedirs(MOVE_TARGET_PATH)
            new_directory_set = get_directory_set(MOVE_SOURCE_PATH)
            old_directory_set = get_directory_set(MOVE_TARGET_PATH)
            stale_directory_set = old_directory_set - new_directory_set

            log('  Removing stale directories and books...')
            for directory in stale_directory_set:
                try:
                    book_id = int(directory)
                except ValueError:
                    # Ignore the directory if its name isn't a book ID number.
                    continue
                book = Book.objects.filter(gutenberg_id=book_id)
                book.delete()
                path = os.path.join(MOVE_TARGET_PATH, directory)
                shutil.rmtree(path)

            log('  Replacing old catalog files...')
            with open(os.devnull, 'w') as null:
                with open(LOG_PATH, 'a') as log_file:
                    call(
                        [
                            'rsync',
                            '-va',
                            '--delete-after',
                            MOVE_SOURCE_PATH + '/',
                            MOVE_TARGET_PATH
                        ],
                        stdout=null,
                        stderr=log_file
                    )

            log('  Putting the catalog in the database...')
            stat_cache = load_stat_cache()
            stat_cache, seen_ids = put_catalog_in_db(stat_cache)
            stat_cache = {k: v for k, v in stat_cache.items() if k in seen_ids}
            save_stat_cache(stat_cache)

            log('  Removing temporary files...')
            shutil.rmtree(TEMP_PATH)

            log('Done!\n')
        except Exception as error:
            error_message = str(error)
            log('Error:', error_message)
            log('')
            shutil.rmtree(TEMP_PATH)

        send_log_email()
