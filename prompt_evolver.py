# prompt_evolver.py
import os, threading, re, json, logging
from datetime import datetime
from google import genai

class PromptEvolver:
    _lock = threading.Lock()

    def __init__(self, model="gemini-2.0-flash", max_calls_per_month=1000, api_key_env="GENAI_API_KEY"):
        self.logger = logging.getLogger(__name__)
        self.model = model
        self.max_calls = max_calls_per_month

        api_key = os.getenv(api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing GenAI API key in env var {api_key_env}")
        self.client = genai.Client(api_key=api_key)

        self.usage_file = os.path.expanduser("~/.prompt_evolver_usage")
        self._load_usage()

    def _load_usage(self):
        self.month = datetime.utcnow().strftime("%Y-%m")
        try:
            with open(self.usage_file) as f:
                m, c = f.read().split()
                self.count = int(c) if m == self.month else 0
        except Exception:
            self.count = 0

    def _save_usage(self):
        with open(self.usage_file, "w") as f:
            f.write(f"{self.month} {self.count}")

    def should_refine(self, prompt: str) -> bool:
        return len(prompt) > 20 and self.count < self.max_calls

    def evolve_prompt(self, prompt: str) -> str:
        with self._lock:
            if self.count >= self.max_calls:
                return prompt

            system_instruction = (
                "Rewrite this user prompt to be as clear and specific as possible, "
                "and break it into up to three sub-prompts named by category. "
                "Return *only* a JSON object with keys "
                "'refined_prompt' (string) and 'sub_prompts' (array of {category, text})."
            )
            full_input = f"{system_instruction}\n\nUser Prompt: \"{prompt}\""

            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=full_input
                )
                text = response.text.strip()
                self.logger.debug(f"[PromptEvolver] raw response: {text!r}")

                if text.startswith("```"):
                    text = re.sub(r"^```[^\n]*\n", "", text)
                    text = re.sub(r"\n```$", "", text)

                m = re.search(r"(\{[\s\S]*\})", text)
                if not m:
                    raise ValueError("no JSON object found in response")

                obj = json.loads(m.group(1))
                refined = obj.get("refined_prompt", prompt)

            except Exception as e:
                self.logger.error(f"[PromptEvolver] API parsing error: {e}")
                refined = prompt

            self.count += 1
            self._save_usage()
            return refined
