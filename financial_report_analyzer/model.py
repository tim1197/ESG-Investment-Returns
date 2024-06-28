import numpy as np
import pandas as pd
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline


MODEL_TYPES = {
    "environmental": "ESGBERT/EnvironmentalBERT-environmental",
    "social": "ESGBERT/SocialBERT-social",
    "governance": "ESGBERT/GovernanceBERT-governance",
}


class ScoringModel:
    def __init__(self):
        env_pipe = self.load_pipe("environmental")
        soc_pipe = self.load_pipe("social")
        gov_pipe = self.load_pipe("governance")
        self.pipes = {
            "environmental": env_pipe,
            "social": soc_pipe,
            "governance": gov_pipe,
        }

    def load_pipe(self, type):
        name = MODEL_TYPES[type]
        tokenizer = AutoTokenizer.from_pretrained(name)
        model = AutoModelForSequenceClassification.from_pretrained(name)
        return pipeline("text-classification", model=model, tokenizer=tokenizer)

    def calculate_report_scores(self, sentences: list) -> dict:
        pd.set_option("future.no_silent_downcasting", True)

        pipe_results = {}

        for pipe in tqdm(self.pipes, ncols=60):
            result = self.pipes[pipe](sentences, padding=True, truncation=True)
            pipe_results[pipe] = pd.DataFrame(result)["label"]

        labels = pd.DataFrame(pipe_results).replace("none", np.nan)
        return (~labels.isna()).astype(int).mean().to_dict()
