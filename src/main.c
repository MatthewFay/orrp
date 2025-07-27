#include <stdio.h>
#include "core/bitmaps.h" // Include your wrapper

int main()
{
    printf("Hello from the database!\n");

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
        if (!bitmap_contains(my_bitmap, 50))
        {
            printf("Bitmap does not contain 50.\n");
        }

        bitmap_free(my_bitmap);
    }

    return 0;
}