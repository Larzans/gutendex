from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0016_person_birth_death_dates'),
    ]

    operations = [
        migrations.AddField(
            model_name='book',
            name='issued_date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='book',
            name='gt_modified',
            field=models.DateField(blank=True, null=True),
        ),
    ]
