import pandas as pd
import numpy as np
from sklearn.datasets import load_diabetes

# Załaduj dane oryginalne, żeby mieć strukturę
diabetes = load_diabetes()
X_original = pd.DataFrame(diabetes.data, columns=diabetes.feature_names)

# Funkcja do generowania losowych danych (dynamiczna)
def generate_random_diabetes_data(n_rows=10, seed=None):
    if seed is not None:
        np.random.seed(seed)

    # Użyj oryginalnych danych jako wzorca
    means = X_original.mean()
    stds = X_original.std()

    # Losowe dane z tego samego rozkładu
    random_data = {
        col: np.random.normal(loc=means[col], scale=stds[col], size=n_rows)
        for col in X_original.columns
    }

    df_random = pd.DataFrame(random_data)
    return df_random
