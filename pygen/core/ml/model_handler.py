import numpy as np

class ModelWrapper:
    def __init__(self, model):
        """
        Wrapper universal para modelos de machine learning e deep learning.
        """
        self.model = model

    def predict(self, X):
        """
        Chama predict, se existir. Caso contrário, tenta inferência.
        """
        if hasattr(self.model, "predict") and callable(self.model.predict):
            return self.model.predict(X)
        elif hasattr(self.model, "__call__"):
            return self.model(X)
        else:
            raise NotImplementedError("O modelo não possui método de predição reconhecido.")

    def predict_proba(self, X):
        """
        Chama predict_proba se existir. Para modelos como Torch, tenta aplicar softmax.
        """
        if hasattr(self.model, "predict_proba") and callable(self.model.predict_proba):
            return self.model.predict_proba(X)
        elif hasattr(self.model, "predict") and callable(self.model.predict):
            try:
                logits = self.model.predict(X)
                return self._softmax(logits)
            except:
                raise AttributeError("Modelo não suporta predict_proba.")
        raise AttributeError("Modelo não suporta predict_proba.")

    def _softmax(self, logits):
        exp = np.exp(logits - np.max(logits, axis=1, keepdims=True))
        return exp / np.sum(exp, axis=1, keepdims=True)

    @property
    def feature_importances_(self):
        """
        Retorna feature importance se o modelo tiver. 
        (Expansível para SHAP, PermutationImportance no futuro)
        """
        if hasattr(self.model, "feature_importances_"):
            return self.model.feature_importances_
        elif hasattr(self.model, "coef_"):  # modelos lineares
            return np.abs(self.model.coef_).flatten()
        else:
            raise AttributeError("Modelo não fornece importâncias de features diretamente.")

    def get_feature_names(self, input_data=None):
        """
        Retorna os nomes das features, se possível (útil para shap ou logs).
        """
        if hasattr(self.model, "feature_names_in_"):
            return self.model.feature_names_in_
        elif input_data is not None and hasattr(input_data, "columns"):
            return input_data.columns
        else:
            return [f"feature_{i}" for i in range(getattr(input_data, "shape", (0,))[1])]

    def get_model_type(self):
        """
        Tenta inferir o tipo do modelo: 'sklearn', 'torch', 'transformer', etc.
        """
        name = type(self.model).__name__.lower()
        if "torch" in name or "nnmodule" in name:
            return "torch"
        elif "pipeline" in name:
            return "sklearn_pipeline"
        elif "booster" in name:
            return "xgboost"
        return "generic"
