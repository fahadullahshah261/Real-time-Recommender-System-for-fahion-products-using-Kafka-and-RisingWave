"""
Utility functions to use CLIP in the Streamlit app. Check the README for details.

"""
import requests
from io import BytesIO
import torch
from PIL import Image
import pandas as pd
from transformers import CLIPProcessor, CLIPModel


# Function that computes the feature vectors for an image
# TODO: optimize this function
def encode_image(model, processor, image_url: str, device='cpu'):
    try:
        response = requests.get(image_url)
        response.raise_for_status()  # Check for HTTP request errors
        image = Image.open(BytesIO(response.content))

        with torch.no_grad():
            photo_preprocessed = processor(text=None, images=image, return_tensors="pt", padding=True)["pixel_values"]
            search_photo_feature = model.get_image_features(photo_preprocessed.to(device))
            search_photo_feature /= search_photo_feature.norm(dim=-1, keepdim=True)

        return search_photo_feature.cpu().numpy()
    except Exception as e:
        print(f"Error processing image {image_url}: {e}")
        return None  # Return None on error