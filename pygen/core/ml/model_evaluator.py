from sklearn.metrics import *
from sklearn.base import ClassifierMixin, RegressorMixin, ClusterMixin
import numpy as np
from sklearn.utils.multiclass import type_of_target


def evaluate(model, X_eval, y_true, y_pred=None):
    """
    Evaluate a model on a given dataset and return a dictionary of metrics.

    Args:

        model (estimator): A scikit-learn estimator.
        X_eval (array-like): Features to evaluate on.
        y_true (array-like): Labels to evaluate on.
        y_pred (array-like, optional): If not given, `model.predict(X_eval)` is used.

    Returns:
        dict: A dictionary containing the following metrics:
            - Classification: accuracy, precision, recall, f1_score, balanced_accuracy,
                cohen_kappa, matthews_corrcoef, zero_one_loss, roc_auc, log_loss,
                average_precision, brier_score_loss (if applicable)
            - Regression: r2_score, mse, rmse, mae, mape, max_error, explained_variance
            - Clustering: silhouette_score, calinski_harabasz_score, davies_bouldin_score (if applicable)

    Raises:
        ValueError: If `model_type` is not one of "classification", "regression", or "clustering".
    """
    metrics = {}
    model_type = detect_model_type(model)

    if y_pred is None:
        y_pred = model.predict(X_eval)

    if model_type == "classification":
        average_type = "binary" if type_of_target(y_true) == "binary" else "macro"

        metrics.update({
            "accuracy": accuracy_score(y_true, y_pred),
            "precision": precision_score(y_true, y_pred, average=average_type, zero_division=0),
            "recall": recall_score(y_true, y_pred, average=average_type, zero_division=0),
            "f1_score": f1_score(y_true, y_pred, average=average_type, zero_division=0),
            "balanced_accuracy": balanced_accuracy_score(y_true, y_pred),
            "cohen_kappa": cohen_kappa_score(y_true, y_pred),
            "matthews_corrcoef": matthews_corrcoef(y_true, y_pred),
            "zero_one_loss": zero_one_loss(y_true, y_pred),
        })

        if hasattr(model, "predict_proba"):
            y_proba = model.predict_proba(X_eval)
            try:
                if y_proba.shape[1] == 2:
                    y_score = y_proba[:, 1]
                else:
                    y_score = y_proba
                metrics["roc_auc"] = roc_auc_score(y_true, y_score, multi_class="ovr")
                metrics["log_loss"] = log_loss(y_true, y_score)
                metrics["average_precision"] = average_precision_score(y_true, y_score)
                metrics["brier_score_loss"] = brier_score_loss(y_true, y_score if y_score.ndim == 1 else y_score[:, 1])
            except Exception:
                pass  # skip proba-based scores if shape mismatch or error occurs

    elif model_type == "regression":
        metrics.update({
            "r2_score": r2_score(y_true, y_pred),
            "mse": mean_squared_error(y_true, y_pred),
            "rmse": np.sqrt(mean_squared_error(y_true, y_pred)),
            "mae": mean_absolute_error(y_true, y_pred),
            "mape": mean_absolute_percentage_error(y_true, y_pred),
            "max_error": max_error(y_true, y_pred),
            "explained_variance": explained_variance_score(y_true, y_pred),
        })

    elif model_type == "clustering":
        try:
            metrics["silhouette_score"] = silhouette_score(X_eval, y_pred)
            metrics["calinski_harabasz_score"] = calinski_harabasz_score(X_eval, y_pred)
            metrics["davies_bouldin_score"] = davies_bouldin_score(X_eval, y_pred)
        except Exception:
            pass

    else:
        raise ValueError(f"Unsupported model type: {model_type}")

    return metrics

def detect_model_type(model):
    if isinstance(model, ClassifierMixin):
        return "classification"
    elif isinstance(model, RegressorMixin):
        return "regression"
    elif isinstance(model, ClusterMixin):
        return "clustering"
    else:
        raise ValueError(f"Unknown model type: {type(model)}")
