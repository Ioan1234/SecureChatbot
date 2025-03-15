import sys
import traceback
import os
import json
import logging
import argparse
import numpy as np
import tensorflow as tf
from datetime import datetime
from sklearn.model_selection import KFold
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint, ReduceLROnPlateau

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("training.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def augment_text_data_simple(texts, labels, augmentation_factor=0.3):
    from random import choice, random, shuffle, randint

    augmented_texts = texts.copy()
    augmented_labels = labels.copy()

    num_to_augment = int(len(texts) * augmentation_factor)
    indices_to_augment = list(range(len(texts)))
    shuffle(indices_to_augment)
    indices_to_augment = indices_to_augment[:num_to_augment]

    logger.info(f"Augmenting {num_to_augment} examples with simple methods")

    replacements = {
        "show": ["display", "list", "get", "find", "retrieve"],
        "list": ["show", "display", "get", "enumerate"],
        "find": ["locate", "get", "search for", "show"],
        "display": ["show", "present", "list", "get"],
        "get": ["retrieve", "obtain", "fetch", "show"],
        "all": ["every", "each", "the complete set of", "the full list of"],
        "where": ["with", "having", "that have", "with the condition"],
        "equal to": ["is", "=", "matching", "that is"],
        "sorted by": ["ordered by", "arranged by", "in order of"],
        "recent": ["latest", "newest", "current", "fresh"],
        "markets": ["exchanges", "trading venues", "market places"],
        "traders": ["users", "trading accounts", "people trading"],
        "trades": ["transactions", "exchanges", "deals"],
        "brokers": ["agents", "intermediaries", "broker firms"],
        "assets": ["securities", "instruments", "holdings", "investments"],
        "highest": ["maximum", "top", "greatest", "largest", "biggest"],
        "lowest": ["minimum", "bottom", "smallest", "least"],
        "middle": ["median", "average", "mid-range", "center"],
        "ascending": ["increasing", "low to high", "smallest to largest"],
        "descending": ["decreasing", "high to low", "largest to smallest"]
    }

    for idx in indices_to_augment:
        text = texts[idx]
        label = labels[idx]

        transform_method = choice(["replace_word", "word_order", "word_removal"])

        if transform_method == "replace_word":
            for original, alternatives in replacements.items():
                if original in text.lower():
                    alternative = choice(alternatives)
                    if text.find(original) >= 0:
                        augmented_text = text.replace(original, alternative)
                    elif text.find(original.capitalize()) >= 0:
                        augmented_text = text.replace(original.capitalize(), alternative.capitalize())
                    else:
                        augmented_text = text.lower().replace(original, alternative)
                    break
            else:
                continue

        elif transform_method == "word_order" and " " in text:
            words = text.split()
            if len(words) > 3:
                idx1 = randint(1, len(words) - 2)
                words[idx1], words[idx1 + 1] = words[idx1 + 1], words[idx1]
                augmented_text = " ".join(words)
            else:
                continue

        elif transform_method == "word_removal" and " " in text:
            words = text.split()
            if len(words) > 4:
                skip_words = ["a", "the", "and", "or", "for", "with", "me", "to"]
                remove_candidates = []
                for i, word in enumerate(words):
                    if word.lower() in skip_words:
                        remove_candidates.append(i)

                if remove_candidates:
                    remove_idx = choice(remove_candidates)
                    words.pop(remove_idx)
                    augmented_text = " ".join(words)
                else:
                    continue
            else:
                continue
        else:
            continue

        augmented_texts.append(augmented_text)
        augmented_labels.append(label)

    logger.info(f"Data augmentation complete. Original size: {len(texts)}, New size: {len(augmented_texts)}")
    unique_texts = []
    unique_labels = []
    seen = set()

    for text, label in zip(augmented_texts, augmented_labels):
        if text.lower() not in seen:
            seen.add(text.lower())
            unique_texts.append(text)
            unique_labels.append(label)

    logger.info(f"Comparative augmentation complete. Original size: {len(texts)}, New size: {len(unique_texts)}")
    return unique_texts, unique_labels


def augment_comparative_queries(texts, labels):
    from random import choice, shuffle

    augmented_texts = texts.copy()
    augmented_labels = labels.copy()

    comparative_types = ["comparative_highest", "comparative_lowest", "comparative_middle",
                         "sort_ascending", "sort_descending"]

    comparative_indices = [
        i for i, label in enumerate(labels)
        if any(comp_type in label for comp_type in comparative_types)
    ]

    shuffle(comparative_indices)

    comparative_indices = comparative_indices[:int(len(comparative_indices) * 0.3)]

    logger.info(f"Augmenting {len(comparative_indices)} comparative queries")

    superlative_replacements = {
        "highest": ["maximum", "greatest", "largest", "top", "best"],
        "lowest": ["minimum", "smallest", "least", "bottom", "worst"],
        "middle": ["median", "average", "mid-range", "center"],
        "ascending": ["increasing", "growing", "rising", "upward"],
        "descending": ["decreasing", "falling", "downward", "reducing"]
    }

    entity_replacements = {
        "price": ["cost", "value", "worth", "rate"],
        "value": ["amount", "total", "sum", "worth"],
        "trades": ["transactions", "deals", "exchanges"],
        "assets": ["stocks", "securities", "instruments", "investments"],
        "traders": ["users", "clients", "customers", "accounts"]
    }

    for idx in comparative_indices:
        text = texts[idx]
        label = labels[idx]

        replace_type = choice(["superlative", "entity", "attribute"])

        augmented = False
        if replace_type == "superlative":
            for term, alternatives in superlative_replacements.items():
                if term in text.lower():
                    alternative = choice(alternatives)
                    augmented_text = text.replace(term, alternative)
                    if term.capitalize() in text:
                        augmented_text = text.replace(term.capitalize(), alternative.capitalize())
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break

        elif replace_type == "entity" and not augmented:
            for entity, alternatives in entity_replacements.items():
                if entity in text.lower():
                    alternative = choice(alternatives)
                    augmented_text = text.replace(entity, alternative)
                    if entity.capitalize() in text:
                        augmented_text = text.replace(entity.capitalize(), alternative.capitalize())
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break

        elif replace_type == "attribute" and not augmented:
            words = text.split()
            for i, word in enumerate(words):
                if word.lower() in entity_replacements:
                    alternatives = entity_replacements[word.lower()]
                    alternative = choice(alternatives)
                    if word[0].isupper():
                        alternative = alternative.capitalize()
                    words[i] = alternative
                    augmented_text = " ".join(words)
                    augmented_texts.append(augmented_text)
                    augmented_labels.append(label)
                    augmented = True
                    break