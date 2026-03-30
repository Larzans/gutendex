from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0015_person_wikipedia_loc_viaf_urls'),
    ]

    operations = [
        migrations.AddField(
            model_name='person',
            name='birth_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='person',
            name='death_date',
            field=models.DateField(blank=True, null=True),
        ),
    ]
