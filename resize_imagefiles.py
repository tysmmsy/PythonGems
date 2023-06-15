from PIL import Image

def resize_image(input_image_path, output_image_path, size):
    original_image = Image.open(input_image_path)
    width, height = original_image.size
    print(f"The original image size is {width} wide x {height} tall")

    # Convert image to RGB if it is RGBA
    if original_image.mode == 'RGBA':
        rgb_image = original_image.convert('RGB')
    else:
        rgb_image = original_image

    # Resize the image so that the longer side is the target size
    ratio = max(size) / max(rgb_image.size)
    new_size = tuple([round(x*ratio) for x in rgb_image.size])
    resized_image = rgb_image.resize(new_size, Image.ANTIALIAS)

    # Create a new image of the target size
    new_image = Image.new("RGB", size)

    # Paste the resized image into the center of the new image
    new_image.paste(resized_image, ((size[0]-resized_image.size[0])//2,
                                    (size[1]-resized_image.size[1])//2))

    print(f"The resized image size is {new_image.size[0]} wide x {new_image.size[1]} tall")
    new_image.save(output_image_path)

if __name__ == "__main__":
    resize_image("input.jpg", "output.jpg", (240, 240))
