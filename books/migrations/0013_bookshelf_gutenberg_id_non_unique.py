from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('books', '0012_bookshelf_gutenberg_id_integerfield'),
    ]

    operations = [
        migrations.AlterField(
            model_name='bookshelf',
            name='gutenberg_id',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
