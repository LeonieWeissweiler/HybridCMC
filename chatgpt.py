
import os
import time

from functools import wraps


OPENAI_ERROR_SLEEP_TIME = 5
API_KEY = None

CACHE_DIR = "cache"  # Directory to store the cached responses
os.makedirs(CACHE_DIR, exist_ok=True)

import random
random.seed(42)

import errno
import signal
import functools

import hashlib


def short_hash(input_str):
    return hashlib.sha256(input_str.encode()).hexdigest()[:8]

# OpenAI
from openai import OpenAI
client = OpenAI(api_key=API_KEY)


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        return wrapper
    return decorator


class Agent:
    """ This is a parent class for all of the agents in the system. 
        It supports gpt from openAI and Anthropic respectively.
    """
    def __init__(self, model, seed=42, sleep=10) -> None:
        self.model = model
        self.seed = seed
        self.sleep = sleep

        self.model_costs_per_1000_tokens = {
            "gpt-4-0125-preview": {
                "input": 0.01,
                "output": 0.03
            },
            # "gpt-4": {
            #     "input": 0.03,
            #     "output": 0.06
            # },
            "gpt-3.5-turbo-1106": {
                "input": 0.001,
                "output": 0.002
            }
        }


    def get_model_cost(self, llm_response):
        """ Get the cost of the model response in USD. """
        prompt_tokens = llm_response.usage.prompt_tokens
        completion_tokens = llm_response.usage.completion_tokens
        cost = (prompt_tokens * self.model_costs_per_1000_tokens[self.model]["input"] + 
                completion_tokens * self.model_costs_per_1000_tokens[self.model]["output"]) / 1000
        return cost
    
    def get_openAI_model_response(self, prompt, system_prompt=None, return_cost=False):
        if system_prompt is None:
            messages = [{"role": "user", "content": prompt}]
        else:
            messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}]

        while True:
            try:
                llm_response = self.call_openAI_API(messages)
                response = llm_response.choices[0].message.content.strip()
                cost = self.get_model_cost(llm_response)
                if return_cost:
                    return response, cost
                else:
                    return response
                
            except Exception as e:
                print(e)
                print(f"Sleep for {self.sleep}s.")
                time.sleep(self.sleep)
                continue

    @timeout(40)
    def call_openAI_API(self, messages):
        llm_response = client.chat.completions.create(model=self.model,
        seed=self.seed,
        messages=messages)
        return llm_response
    


def cache_result(func):
    @wraps(func)
    def wrapper(prompt, system_prompt, model,  ignore_cache=False):
        if ignore_cache:
            return func(prompt, system_prompt, model)
        cache_name = short_hash(prompt + system_prompt + model)
        cache_file = os.path.join(CACHE_DIR, f"{func.__name__}_{cache_name}.json")

        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                cost = float(f.readline().strip())
                result = f.read()
                
                return result, cost
        else:
            result, cost = func(prompt, system_prompt, model=model)
            with open(cache_file, "w") as f:
                f.write(str(cost) + "\n")
                f.write(result)
                

            return wrapper(prompt, system_prompt, model)

    return wrapper

@cache_result
def run_prompt(prompt, system_prompt, model):
    agent = Agent(model)
    response, cost = agent.get_openAI_model_response(prompt, system_prompt=system_prompt, return_cost=True)
    return response, cost

