import ollama
import json
import logging

logger = logging.getLogger("BPS_AI_Engine")

class BPS_AI_Engine:
    def __init__(self):
        self.models = {
            "naker": "bps-naker",
            "lnprt": "bps-lnprt",
            "bmei": "bmei-auditor"
        }

    def _execute_query(self, model_key, custom_prompt):
        """Transport layer untuk mengirim prompt ke Ollama."""
        try:
            response = ollama.chat(
                model=self.models[model_key],
                messages=[{'role': 'user', 'content': custom_prompt}],
                format='json',
                options={'temperature': 0.1} # Menjaga konsistensi sesuai SOP BPS
            )
            return json.loads(response['message']['content'])
        except Exception as e:
            logger.error(f"Koneksi Ollama Gagal ({model_key}): {e}")
            return None

    def classify_naker(self, custom_prompt):
        return self._execute_query("naker", custom_prompt)

    def classify_lnprt(self, text):
        """Digunakan oleh lnprt_scraper.py"""
        return self._interrogate_model(self.models["lnprt"], text)

    def audit_bmei(self, text):
        """Digunakan oleh bmei_scraper.py"""
        return self._interrogate_model(self.models["bmei"], text)