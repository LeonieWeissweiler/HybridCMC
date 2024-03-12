import os
import pandas as pd
from vllm import LLM, SamplingParams
import argparse
from tqdm import tqdm
import json
import pandas as pd
import spacy
import json

SYSTEM_PROMPT = ""
# dataset = pd.read_csv("stitched.tsv", sep="\t", header=0)


parser = argparse.ArgumentParser()
parser.add_argument("--num_gpus", type=int, default=2)
parser.add_argument("--model_name", type=str, default="mistralai/Mistral-7B-Instruct-v0.1")
args = parser.parse_args()

num_gpus = args.num_gpus
model_name = args.model_name

cache_dir = None
sampling_params = SamplingParams(temperature=0, max_tokens=30)

if "AWQ" in model_name:
    llm = LLM(model=model_name, download_dir=cache_dir, tensor_parallel_size=num_gpus, gpu_memory_utilization=0.85, quantization="AWQ")
else:
    llm = LLM(model=model_name, download_dir=cache_dir, tensor_parallel_size=num_gpus, gpu_memory_utilization=0.85)

def get_model_answer(prompts):
    outputs_raw = llm.generate(prompts, sampling_params)
    outputs = []
    for o in outputs_raw:
        outputs.append(vars(o.outputs[0])["text"].strip())
    return outputs

with open("all_sents.json", "r") as f:
    sentences_for_model = json.load(f)

model_answers = get_model_answer(sentences_for_model)
model_answers_dict = {}
for sentence, answer in zip(sentences_for_model, model_answers):
    model_answers_dict[sentence] = answer

with open("results5/"+model_name.replace("/","_")+"_model_outputs.json", "w") as f:
    json.dump(model_answers_dict, f)
