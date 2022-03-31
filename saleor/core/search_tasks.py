from typing import List

from celery.utils.log import get_task_logger

from ..account.models import User
from ..account.search import prepare_user_search_document_value
from ..celeryconf import app
from ..order.models import Order
from ..order.search import prepare_order_search_document_value
from ..product.models import Product
from ..product.search import (
    PRODUCT_FIELDS_TO_PREFETCH,
    prepare_product_search_document_value,
)

task_logger = get_task_logger(__name__)

BATCH_SIZE = 1000


@app.task
def set_user_search_document_values(updated_count: int = 0) -> None:
    qs = list(
        User.objects.filter(search_document="")
        .prefetch_related("addresses")[:BATCH_SIZE]
        .iterator()
    )

    if not qs:
        task_logger.info("No users to update.")
        return

    updated_count += set_search_document_values(qs, prepare_user_search_document_value)

    task_logger.info("Updated %d {Model.__name__.lower()}s", updated_count)

    if len(qs) < BATCH_SIZE:
        task_logger.info("Setting user search document values finished.")
        return

    del qs

    set_user_search_document_values.delay(updated_count)


@app.task
def set_order_search_document_values(updated_count: int = 0) -> None:
    qs = list(
        Order.objects.filter(search_document="")
        .prefetch_related(
            "user",
            "billing_address",
            "shipping_address",
            "payments",
            "discounts",
            "lines",
        )[:BATCH_SIZE]
        .iterator()
    )

    if not qs:
        task_logger.info("No orders to update.")
        return

    updated_count += set_search_document_values(qs, prepare_order_search_document_value)

    task_logger.info("Updated %d {Model.__name__.lower()}s", updated_count)

    if len(qs) < BATCH_SIZE:
        task_logger.info("Setting order search document values finished.")
        return

    del qs

    set_order_search_document_values.delay(updated_count)


@app.task
def set_product_search_document_values(updated_count: int = 0) -> None:
    qs = list(
        Product.objects.filter(search_document="")
        .prefetch_related(*PRODUCT_FIELDS_TO_PREFETCH)[:BATCH_SIZE]
        .iterator()
    )

    if not qs:
        task_logger.info("No products to update.")
        return

    updated_count += set_search_document_values(
        qs, prepare_product_search_document_value
    )

    task_logger.info("Updated %d {Model.__name__.lower()}s", updated_count)

    if len(qs) < BATCH_SIZE:
        task_logger.info("Setting product search document values finished.")
        return

    del qs

    set_product_search_document_values.delay(updated_count)


def set_search_document_values(instances: List, prepare_search_document_func):
    if not instances:
        return
    Model = instances[0]._meta.Model
    for instance in instances:
        instance.search_document = prepare_search_document_func(
            instance, already_prefetched=True
        )
    Model.objects.bulk_update(instances, ["search_document"])

    return len(instances)
