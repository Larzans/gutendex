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


def put_catalog_in_db(stat_cache):
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

    # Pre-load small lookup tables (bookshelves, languages) into memory.
    # Subjects use a lazy cache — built during the run to avoid loading all
    # 40k+ rows upfront while still avoiding repeated DB hits for common values.
    bookshelf_cache = {b.name: b for b in Bookshelf.objects.all()}
    language_cache  = {l.code: l for l in Language.objects.all()}
    subject_cache   = {}
    log('  Caches loaded:  bookshelves=%d  languages=%d' % (
        len(bookshelf_cache), len(language_cache)))

    skipped = 0
    processed = 0
    total_start = time()
    batch_start = time()

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

        book = utils.get_book(id, book_path)

        try:
            '''Make/update the book.'''

            # Single query: .first() returns the object or None.
            book_in_db = Book.objects.filter(gutenberg_id=id).first()
            related_books_str = ','.join(str(i) for i in book['related_books'])

            if book_in_db is not None:
                book_in_db.copyright = book['copyright']
                book_in_db.download_count = book['downloads']
                book_in_db.media_type = book['type']
                book_in_db.title = book['title']
                book_in_db.published_year = book['published_year']
                book_in_db.wikipedia_url = book['wikipedia_url']
                book_in_db.reading_score = book['reading_score']
                book_in_db.reading_score_value = book['reading_score_value']
                book_in_db.related_books = related_books_str
                book_in_db.save()
            else:
                book_in_db = Book.objects.create(
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
                )

            ''' Make/update the authors. '''

            book_in_db.authors.set(
                [get_or_create_person(a) for a in book['authors']],
                clear=True,
            )

            ''' Make/update the editors. '''

            # Skip set() entirely when empty — avoids a needless DELETE query
            # (most books have no editors or translators).
            editors = [get_or_create_person(e) for e in book['editors']]
            if editors:
                book_in_db.editors.set(editors, clear=True)
            else:
                book_in_db.editors.clear()

            ''' Make/update the translators. '''

            translators = [get_or_create_person(t) for t in book['translators']]
            if translators:
                book_in_db.translators.set(translators, clear=True)
            else:
                book_in_db.translators.clear()

            ''' Make/update the book shelves. '''

            bookshelves = []
            for shelf in book['bookshelves']:
                if shelf not in bookshelf_cache:
                    bookshelf_cache[shelf] = Bookshelf.objects.create(name=shelf)
                bookshelves.append(bookshelf_cache[shelf])

            book_in_db.bookshelves.set(bookshelves, clear=True)

            ''' Make/update the formats. '''

            # Load all existing formats for this book in one query, key by
            # (mime_type, url) so membership checks are O(1) in Python.
            existing_formats = {
                (f.mime_type, f.url): f
                for f in Format.objects.filter(book=book_in_db)
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

            book_in_db.languages.set(languages, clear=True)

            ''' Make/update subjects. '''

            subjects = []
            for subject in book['subjects']:
                if subject not in subject_cache:
                    subject_cache[subject], _ = Subject.objects.get_or_create(name=subject)
                subjects.append(subject_cache[subject])

            book_in_db.subjects.set(subjects, clear=True)

            ''' Make/update summaries. '''

            # Same pattern as formats: load existing in one query, compare in
            # Python, bulk_create new ones, bulk delete stale ones.
            existing_summaries = {
                s.text: s for s in Summary.objects.filter(book=book_in_db)
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

    return stat_cache, {str(id) for id in book_ids}


def get_or_create_person(data):
    gid = data.get('gutenberg_id')
    if gid:
        person = Person.objects.filter(gutenberg_id=gid).first()
        if person:
            Person.objects.filter(pk=person.pk).update(
                name=data['name'],
                birth_year=data['birth'],
                death_year=data['death'],
            )
            return person
        return Person.objects.create(
            gutenberg_id=gid,
            name=data['name'],
            birth_year=data['birth'],
            death_year=data['death'],
        )
    # Fallback: no agent ID in RDF (rare)
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
