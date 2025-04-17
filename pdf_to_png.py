import argparse
import os
from pdf2image import convert_from_path
from pdf2image.exceptions import PDFInfoNotInstalledError, PDFPageCountError, PDFSyntaxError

def convert_pdf_to_png(pdf_path, output_dir=None):
    """
    Converts each page of a PDF file to a PNG image.

    Args:
        pdf_path (str): The path to the input PDF file.
        output_dir (str, optional): The directory to save the PNG files.
                                     Defaults to the directory of the PDF file.
    """
    if not os.path.exists(pdf_path):
        print(f"Error: PDF file not found at {pdf_path}")
        return

    if not output_dir:
        output_dir = os.path.dirname(pdf_path)
        if not output_dir: # Handle case where only filename is given
             output_dir = '.'

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    try:
        print(f"Converting {pdf_path} to PNG images...")
        # Convert PDF to a list of PIL images
        images = convert_from_path(pdf_path)

        # Get the base name of the PDF file without extension
        base_filename = os.path.splitext(os.path.basename(pdf_path))[0]

        # Save each image as a PNG file
        for i, image in enumerate(images):
            output_filename = os.path.join(output_dir, f"{base_filename}_page_{i + 1}.png")
            image.save(output_filename, 'PNG')
            print(f"Saved page {i + 1} to {output_filename}")

        print("Conversion complete.")

    except PDFInfoNotInstalledError:
        print("Error: pdf2image requires poppler to be installed and in PATH.")
        print("Please install poppler:")
        print("  macOS (brew): brew install poppler")
        print("  Debian/Ubuntu: sudo apt-get install poppler-utils")
        print("  Windows: Download from https://github.com/oschwartz10612/poppler-windows/releases/")
    except PDFPageCountError:
        print(f"Error: Could not get page count for {pdf_path}. Is it a valid PDF?")
    except PDFSyntaxError:
        print(f"Error: PDF file {pdf_path} seems to be corrupted or invalid.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a PDF file to PNG images (one image per page).")
    parser.add_argument("pdf_file", help="Path to the input PDF file.")
    parser.add_argument("-o", "--output", help="Directory to save the output PNG files (defaults to PDF file's directory).")

    args = parser.parse_args()

    convert_pdf_to_png(args.pdf_file, args.output)
