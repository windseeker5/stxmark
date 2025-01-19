from rembg import remove
from PIL import Image, UnidentifiedImageError
import io, os

input_path = "logos"
output_path = "logos_output"
folder_path = "logos"


def normalize_filenames(folder_path):
    for filename in os.listdir(folder_path):
        new_filename = filename.replace('(', '_').replace(')', '')
        old_file = os.path.join(folder_path, filename)
        new_file = os.path.join(folder_path, new_filename)
        os.rename(old_file, new_file)
        print(f"Renamed: {old_file} -> {new_file}")


def process_images(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for filename in os.listdir(input_folder):
        input_file = os.path.join(input_folder, filename)
        output_file = os.path.join(output_folder, os.path.splitext(filename)[0] + ".png")

        try:
            with open(input_file, "rb") as i:
                input_image = i.read()
                output_image = remove(input_image)

            img = Image.open(io.BytesIO(output_image)).convert("RGBA")
            img.save(output_file, "PNG")
            print(f"Processed: {input_file} -> {output_file}")
        except UnidentifiedImageError:
            print(f"Skipped: {input_file} (Unidentified image file)")


if __name__ == "__main__":
    normalize_filenames(folder_path)
    process_images(folder_path, output_path)