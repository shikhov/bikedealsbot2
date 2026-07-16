import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from constants import PRICE_ROLLBACK_TOLERANCE, PRICE_ROLLBACK_WINDOW_SECONDS
from models import Sku, Variant


@pytest.fixture
def make_sku():
    def factory(price_history):
        variant = Variant({
            'store': 'TEST',
            'prodid': 'product',
            'skuid': 'sku',
            'url': 'https://example.com/product',
            'name': 'Test product',
            'variant': '',
            'price': price_history[-1]['price'],
            'currency': 'RUB',
            'instock': True,
        })
        return Sku(
            variant=variant,
            doc_id='1_TEST_product_sku',
            chat_id='1',
            errors=0,
            enable=True,
            lastcheck='',
            lastcheckts=0,
            lastgoodts=0,
            instock_prev=True,
            price_prev=None,
            price_history=price_history,
        )

    return factory


def test_is_recent_price_rollback(make_sku):
    sku = make_sku([
        {'price': 1_000, 'timestamp': 0},
        {'price': 1_200, 'timestamp': 60},
        {'price': 1_010, 'timestamp': 120},
    ])

    assert sku.is_recent_price_rollback()


def test_is_not_rollback_with_less_than_three_prices(make_sku):
    sku = make_sku([
        {'price': 1_000, 'timestamp': 0},
        {'price': 1_010, 'timestamp': 60},
    ])

    assert not sku.is_recent_price_rollback()


def test_is_not_rollback_outside_price_tolerance(make_sku):
    sku = make_sku([
        {'price': 1_000, 'timestamp': 0},
        {'price': 1_200, 'timestamp': 60},
        {'price': int(1_000 * (1 + PRICE_ROLLBACK_TOLERANCE)) + 1, 'timestamp': 120},
    ])

    assert not sku.is_recent_price_rollback()


def test_is_not_rollback_outside_time_window(make_sku):
    sku = make_sku([
        {'price': 1_000, 'timestamp': 0},
        {'price': 1_200, 'timestamp': 60},
        {'price': 1_000, 'timestamp': PRICE_ROLLBACK_WINDOW_SECONDS + 1},
    ])

    assert not sku.is_recent_price_rollback()
