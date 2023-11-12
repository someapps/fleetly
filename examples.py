import asyncio

from fleetly.fleetly import FleETLy


async def extract_nums():
    for i in range(10):
        await asyncio.sleep(0)
        yield i


def extract_letters():
    data = 'abcdefg'

    for i in range(len(data)):
        yield data[i]


async def transform_mult(item):
    yield item * 2
    yield item * 3


def transform_change(item):
    yield item + '_'
    yield '_' + item


async def transform_filter(item):
    if item.endswith('_'):
        yield item


async def transform_sleep(item):
    await asyncio.sleep(0)
    return item


class AsyncLoader:

    def __init__(self):
        # some dependencies
        pass

    def __call__(self, item):
        print('async load', item)


if __name__ == '__main__':
    # noinspection PyStatementEffect
    def main():
        etl = FleETLy()

        etl >> extract_nums >> transform_mult >> str >> transform_change
        etl >> extract_letters >> transform_change
        etl[transform_change] >> transform_sleep >> transform_filter >> print
        etl[transform_filter] >> AsyncLoader()

        asyncio.run(etl.run())

        etl.make_diagram()

    main()
