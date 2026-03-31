from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("books", "0018_book_stats_fields")]

    operations = [
        migrations.RenameField(
            model_name="book",
            old_name="related_books",
            new_name="related_gt_books",
        ),
        migrations.AddField(
            model_name="book",
            name="se_match_id",
            field=models.CharField(blank=True, default="", max_length=256),
        ),
    ]
