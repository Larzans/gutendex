from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0017_book_issued_date_gt_modified'),
    ]

    operations = [
        migrations.AddField(
            model_name='book',
            name='word_count',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='reading_time_minutes',
            field=models.PositiveIntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='flesch_reading_ease',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='dale_chall_score',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='rare_word_ratio',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='stats_computed_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='stats_failed',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='book',
            name='stats_fail_reason',
            field=models.CharField(blank=True, default='', max_length=512),
        ),
    ]
