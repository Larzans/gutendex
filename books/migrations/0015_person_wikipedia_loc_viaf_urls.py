from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0014_bookshelf_name_non_unique'),
    ]

    operations = [
        migrations.AddField(
            model_name='person',
            name='wikipedia_url',
            field=models.URLField(blank=True, default='', max_length=512),
        ),
        migrations.AddField(
            model_name='person',
            name='loc_url',
            field=models.URLField(blank=True, default='', max_length=512),
        ),
        migrations.AddField(
            model_name='person',
            name='viaf_url',
            field=models.URLField(blank=True, default='', max_length=512),
        ),
    ]
