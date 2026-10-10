import numpy as np


order_size = {
    "class": "GeneralMixtureModel",
    "distributions": [
        {
            "class": "Distribution",
            "name": "LogNormalDistribution",
            "parameters": [2.9, 1.2],
            "frozen": False,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [100.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [200.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [300.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [400.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [500.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [600.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [700.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [800.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [900.0, 0.15],
            "frozen": True,
        },
        {
            "class": "Distribution",
            "name": "NormalDistribution",
            "parameters": [1000.0, 0.15],
            "frozen": True,
        },
    ],
    "weights": [
        0.2,
        0.7,
        0.06,
        0.004,
        0.0329,
        0.001,
        0.0006,
        0.0004,
        0.0005,
        0.0003,
        0.0003,
    ],
}


# NOTE (Agenthon Track 3 fork): the upstream model used pomegranate's
# GeneralMixtureModel, which is unmaintained and does not build on Python 3.11.
# This is a drop-in NumPy reimplementation that samples from exactly the same
# fixed mixture defined in `order_size` above (one LogNormal component plus ten
# frozen Normal components), preserving the public interface and determinism.
class OrderSizeModel:
    _NAME_TO_KIND = {
        "LogNormalDistribution": "lognormal",
        "NormalDistribution": "normal",
    }

    def __init__(self) -> None:
        weights = np.asarray(order_size["weights"], dtype=float)
        self._weights = weights / weights.sum()
        self._kinds: list[str] = []
        self._params: list[tuple[float, float]] = []
        for dist in order_size["distributions"]:
            kind = self._NAME_TO_KIND.get(dist["name"])
            if kind is None:
                raise ValueError(f"unsupported distribution: {dist['name']}")
            a, b = dist["parameters"][0], dist["parameters"][1]
            self._kinds.append(kind)
            self._params.append((float(a), float(b)))

    def sample(self, random_state: np.random.RandomState) -> float:
        idx = random_state.choice(len(self._weights), p=self._weights)
        kind = self._kinds[idx]
        a, b = self._params[idx]
        # pomegranate LogNormalDistribution(mu, sigma): log(x) ~ Normal(mu, sigma);
        # numpy lognormal(mean, sigma) draws exp(Normal(mean, sigma)) -- equivalent.
        if kind == "lognormal":
            value = random_state.lognormal(mean=a, sigma=b)
        else:  # NormalDistribution(mean, std)
            value = random_state.normal(loc=a, scale=b)
        return round(value)
