from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0011_bookshelf_parent_gutenberg_id'),
    ]

    operations = [
        migrations.AlterField(
            model_name='bookshelf',
            name='gutenberg_id',
            field=models.IntegerField(blank=True, null=True, unique=True),
        ),
    ]
