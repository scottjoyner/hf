import pandas as pd
import requests
from tqdm import tqdm
import time
import os
import json
import argparse

# huggingface-electra-small-discriminator,https://sparknlp.org/2022/06/22/electra_qa_hankzhong_small_discriminator_finetuned_squad_en_3_0.html,Apache_2.0,14m

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

HUGGINGFACE_TOKEN = os.getenv("HUGGINGFACE_TOKEN")

HEADERS = {}
if HUGGINGFACE_TOKEN:
    HEADERS["Authorization"] = f"Bearer {HUGGINGFACE_TOKEN}"

def fetch_model_info(model_id: str):
    """
    Calls Hugging Face Model Hub API to retrieve model metadata,
    but caches results in local JSON files to avoid repeated calls.
    """
    cache_path = os.path.join(CACHE_DIR, f"{model_id.replace('/', '__')}.json")

    # Try loading from cache
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                data = json.load(f)
        except Exception:
            data = None
    else:
        data = None

    if data is None:
        api_url = f"https://huggingface.co/api/models/{model_id}"
        try:
            response = requests.get(api_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            data = response.json()

            # Save to cache
            with open(cache_path, "w") as f:
                json.dump(data, f, indent=2)

            time.sleep(0.5)  # polite backoff
        except requests.HTTPError as e:
            print(f"Error fetching {model_id}: {e}")
            return "", "", ""
        except Exception as e:
            print(f"Error fetching {model_id}: {e}")
            return "", "", ""

    # Parse data
    card_data = data.get("cardData", {}) if isinstance(data, dict) else {}
    model_description = card_data.get("summary", data.get("pipeline_tag", "")) if isinstance(data, dict) else ""
    params = card_data.get("params", "")
    size = card_data.get("model_size", "")

    return model_description, params, size

def extract_model_id(url: str):
    """
    Extracts model ID from Hugging Face URL.
    e.g., https://huggingface.co/Cohere/Cohere-embed-english-light-v3.0
    -> Cohere/Cohere-embed-english-light-v3.0
    """
    return "/".join(url.strip("/").split("/")[-2:])

def enrich_csv(input_csv="models.csv", output_csv="models_enriched.csv"):
    df = pd.read_csv(input_csv)
    new_descriptions = []
    new_params = []
    new_sizes = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Enriching"):
        url = row.get('updated_url') or row.get('url')
        if pd.isna(url) or not isinstance(url, str) or not url.startswith("https://huggingface.co"):
            new_descriptions.append("")
            new_params.append("")
            new_sizes.append("")
            continue

        model_id = extract_model_id(url)
        description, params, size = fetch_model_info(model_id)
        new_descriptions.append(description)
        new_params.append(params)
        new_sizes.append(size)
        time.sleep(0.1)  # avoid hammering

    df['model_description'] = new_descriptions
    df['params'] = new_params
    df['model_size'] = new_sizes

    df.to_csv(output_csv, index=False)
    print(f"Enriched data saved to {output_csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/models.csv")
    ap.add_argument("--output", default="models_enriched.csv")
    args = ap.parse_args()
    enrich_csv(args.input, args.output)
