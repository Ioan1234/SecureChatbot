def patch_punkt_tokenizer():
    import nltk
    from nltk.tokenize.punkt import PunktTokenizer

    original_init = PunktTokenizer.__init__

    def patched_init(self, lang='english'):
        self._PunktTokenizer__lang_vars = nltk.tokenize.punkt.PunktLanguageVars()
        self._params = nltk.data.load(
            resource_url=f"tokenizers/punkt/{lang}.pickle"
        )

    PunktTokenizer.__init__ = patched_init
    print("PunktTokenizer patched to use punkt resource instead of punkt_tab")

if __name__ == "__main__":
    patch_punkt_tokenizer()