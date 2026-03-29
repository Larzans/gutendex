from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0013_bookshelf_gutenberg_id_non_unique'),
    ]

    operations = [
        migrations.AlterField(
            model_name='bookshelf',
            name='name',
            field=models.CharField(max_length=64),
        ),
    ]
