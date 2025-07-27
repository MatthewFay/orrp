#include <stdio.h>
#include <stdlib.h>
#include "core/bitmaps.h" // Your Roaring wrapper
#include "core/db.h"      // Your LMDB wrapper

int main()
{
    printf("Hello from the database project!\n");

    // --- Example using Roaring Bitmaps ---
    printf("\n--- Roaring Bitmaps Example ---\n");
    bitmap_t *my_bitmap = bitmap_create();
    if (my_bitmap)
    {
        bitmap_add(my_bitmap, 10);
        bitmap_add(my_bitmap, 20);
        bitmap_add(my_bitmap, 100);

        if (bitmap_contains(my_bitmap, 20))
        {
            printf("Bitmap contains 20.\n");
        }
        else
        {
            printf("Bitmap does NOT contain 20.\n");
        }
        if (!bitmap_contains(my_bitmap, 50))
        {
            printf("Bitmap does not contain 50.\n");
        }
        else
        {
            printf("Bitmap DOES contain 50.\n");
        }

        bitmap_free(my_bitmap);
    }
    else
    {
        fprintf(stderr, "Failed to create bitmap.\n");
    }

    // --- Example using LMDB ---
    printf("\n--- LMDB Example ---\n");
    const char *db_path = "my_database.mdb"; // Name of the LMDB file
    size_t db_map_size = 1048576;            // 1 MB map size

    db_t *my_db = db_open(db_path, db_map_size);
    if (my_db)
    {
        printf("Database opened successfully.\n");

        if (db_put(my_db, "name", "Alice"))
        {
            printf("Put 'name': 'Alice'\n");
        }
        else
        {
            fprintf(stderr, "Failed to put 'name'.\n");
        }

        if (db_put(my_db, "city", "New York"))
        {
            printf("Put 'city': 'New York'\n");
        }
        else
        {
            fprintf(stderr, "Failed to put 'city'.\n");
        }

        char *retrieved_name = db_get(my_db, "name");
        if (retrieved_name)
        {
            printf("Get 'name': '%s'\n", retrieved_name);
            free(retrieved_name); // Remember to free memory returned by db_get
        }
        else
        {
            printf("Key 'name' not found or error.\n");
        }

        char *retrieved_age = db_get(my_db, "age");
        if (retrieved_age)
        {
            printf("Get 'age': '%s'\n", retrieved_age);
            free(retrieved_age);
        }
        else
        {
            printf("Key 'age' not found or error.\n");
        }

        db_close(my_db);
        printf("Database closed.\n");
    }
    else
    {
        fprintf(stderr, "Failed to open database.\n");
    }

    return 0;
}