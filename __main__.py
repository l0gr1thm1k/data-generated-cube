import asyncio

from src.pipeline import DataGeneratedCubePipeline

data_path = "/home/daniel/Code/mtg/data-generated-cube/src/data/cubes/2023_06_23_crawl_test"
pipeline = DataGeneratedCubePipeline(data_save_path=data_path, card_count=360, blacklist_path=None)


if __name__ == '__main__':
    asyncio.run(pipeline.generate_cube())
