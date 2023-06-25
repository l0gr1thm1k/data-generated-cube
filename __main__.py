import asyncio

from src.pipeline import DataGeneratedCubePipeline


if __name__ == '__main__':
    pipeline = DataGeneratedCubePipeline("example_config.json")
    asyncio.run(pipeline.run())
