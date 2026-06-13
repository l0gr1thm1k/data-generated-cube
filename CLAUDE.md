# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Generated Cube is a Python project for generating and analyzing Magic: The Gathering cubes using data from Cube Cobra and Scryfall. The pipeline scrapes cube data, combines it, generates new cubes, and performs statistical analysis.

## Running the Project

### Installation
```sh
pip install -r requirements.txt
```

### Running the Main Pipeline
The main pipeline generates a cube based on a configuration file:

```sh
python __main__.py
```

By default, this runs `src/cube_config/example_configs/example_config.json`. To use a different config, modify line 13 in `__main__.py`:

```python
config_file = str(EXAMPLE_CONFIGS_DIRECTORY_PATH / "your_config.json")
```

### Running Cohort Analysis
For cohort analysis (comparing multiple cubes):

```sh
python cube_cohort_analysis.py
```

By default, this runs `src/cube_config/example_configs/CubeCon2025Cohort.json`. Modify line 13 to change the config.

## Architecture

### Pipeline Architecture
The project uses two main pipelines:

1. **DataGeneratedCubePipeline** (`src/pipeline.py`) - Main cube generation pipeline with three stages:
   - **scrape**: `CubeCobraScraper` downloads cube data from Cube Cobra (via S3 bucket or direct API)
   - **create**: `CubeGenerator` combines cube data and generates a new cube
   - **analyze**: `CubeAnalyzer` produces statistical visualizations and analysis

2. **CohortAnalysisPipeline** (`src/cohort_pipeline.py`) - Analyzes cube cohorts:
   - Scrapes multiple cubes
   - Uses `CohortAnalyzer` to compute complexity metrics across a cohort of cubes

Both pipelines inherit from `PipelineObject` and are async.

### Configuration System
Cube configs are JSON files in `src/cube_config/example_configs/`. The `CubeConfig` class (`src/cube_config/cube_configuration.py`) loads configs and provides dict-like and dot-notation access.

**Key config fields:**
- `cubeName`: Name for the generated cube
- `cubeIds`: List of Cube Cobra IDs to sample from
- `cardCount`: Target cube size (default 360)
- `cubeCategory`: Category like "Vintage", "Pauper", "Peasant"
- `stages`: Which pipeline stages to run (["scrape", "create", "analyze"])
- `useCubeCobraBucket`: Whether to use Cube Cobra's S3 bucket (requires AWS credentials)
- `cohortAnalysis`: Set to `true` for cohort analysis mode
- `cardBlacklist`: Optional list of cards to exclude

### Data Flow
1. **Input**: Configuration JSON specifies source cubes and parameters
2. **Scraping**: Downloads cube lists to `artifacts/data/{cubeName}/`
3. **Combination**: `CubeCombiner` merges all cube CSVs into weighted DataFrame
4. **Generation**: `CubeCreator` samples cards based on frequency weights
5. **Output**: Generated cube saved to `artifacts/results/{cubeName}.csv`
6. **Analysis**: Visualizations saved to `artifacts/analysis/{cubeName}/`

### Key Components

**Scraper** (`src/cube_cobra_crawler/crawler.py`):
- Fetches cube data from Cube Cobra S3 bucket (requires AWS keys) or scrapes from cubecobra.com
- Filters cubes by category and recency
- Generates CSV files via `CSVFileGenerator`

**Cube Generator** (`src/data_generated_cube/cube_generator.py`):
- `CubeCombiner` aggregates cube data with weighting
- `CubeCreator` samples cards proportionally to their appearance frequency
- Integrates with `ScryfallCache` for card data

**Analyzers**:
- `CubeAnalyzer` (`src/cube_analysis/analyzer.py`): Generates color distribution, mana curve, type distribution visualizations
- `CohortAnalyzer` (`src/cohort_analysis/cohort_analysis.py`): Computes cube complexity metrics including keyword diversity, oracle text complexity, instant-speed ratios, and casting cost complexity

**Caching**:
- `shared_scryfall_cache` (`src/data_generated_cube/scryfall/scryfall_cache.py`): Singleton that caches Scryfall API calls
- `ELOFetcher` (`src/data_generated_cube/elo/elo_fetcher.py`): Caches card ELO ratings

### Directory Structure
- `src/` - Source code
  - `common/` - Shared utilities and constants
  - `cube_config/` - Configuration classes and example configs
  - `cube_cobra_crawler/` - Web scraping components
  - `data_generated_cube/` - Cube generation logic
  - `cube_analysis/` - Analysis and visualization
  - `cohort_analysis/` - Cohort comparison analysis
- `artifacts/` - Generated at runtime (gitignored)
  - `data/{cubeName}/` - Downloaded cube CSVs
  - `results/` - Generated cube CSVs
  - `analysis/{cubeName}/` - Analysis visualizations
  - `cohort_analysis/` - Cohort analysis output

## Environment Variables

No environment variables are required. The crawler reads from Cube Cobra's public S3 export bucket (`s3://cubecobra-public/export/`) using anonymous access — set `useCubeCobraBucket: false` in the config to skip the bucket fetch entirely and rely solely on the explicit `cubeIds` list. The export is refreshed quarterly, so downloads are cached locally for 7 days.

## Constants and Configuration

Important constants in `src/common/constants.py`:
- `COLORS_SET`, `CARD_COLOR_MAP`, `COLOR_PALETTE` - MTG color definitions
- `EVERGREEN_KEYWORDS` - Standard MTG keywords
- `BLACKLIST_REGEX` - Filters out specialty cubes (old school, mono-color, etc.)
- `ALTERNATIVE_COST_PATTERN` - Regex for cards with alternative casting costs
- `CUBE_COMPLEXITY_WEIGHTS` - Weights for cohort complexity scoring

## Testing

No test framework is currently configured (no pytest.ini or tests/ directory found).
