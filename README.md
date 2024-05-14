# Data Generated Cube

***

## Overview
Data Generated Cube is a Python project designed to facilitate the generation and analysis of Magic: The Gathering 
cubes from [Cube Cobra](https://cubecobra.com/landing) and [Scryfall](https://scryfall.com/) data  sources. This tool is 
particularly useful for data scientists, analysts and the quantitatively minded enthusiast who seeks a deeper 
understanding of cube construction within the cube format.

I maintain a [cube generated using this pipeline](https://cubecobra.com/cube/overview/data), but it is capable of 
creating cubes of any size and category.

##  Requirements
* Python 3.7 or higher
* Dependencies listed in [requirements.txt](https://github.com/l0gr1thm1k/data-generated-cube/blob/github/requirements.txt)

## Installation
1. Clone the repository:

    ```sh 
    git clone https://github.com/l0gr1thm1k/data-generated-cube.git
   ```

2. Navigate to the project directory. The precise path you use will depend on where you cloned the repository. For 
   example, if you cloned the repository to your home directory, you would use the following command:
    
    ```sh
    cd data-generated-cube
   ```

3. Install the required dependencies:

    ```sh
    pip install -r requirements.txt
   ```
   
## Usage
The pipeline works by doing the following. 

1. Create a cube configuration file.
2. running the `__main__.py` script with the path to your configuration file as an argument.

### Cube Configuration File
The cube creation pipeline takes in a JSON configuration file of the following form. 

```json
{
  "cubeName": "example_data_generated_cube",
  "cardBlacklist": null,
  "cardCount": 360,
  "cubeCategory": "Vintage",
  "cubeIds": [
    "modovintage",
    "wtwlf123",
    "synergy",
    "LSVCubeInit",
    "AlphaFrog"
],
  "overwrite": true,
  "stages": [
    "scrape",
    "create",
    "analyze"
  ],
  "useCubeCobraBucket": true
}
```

You can see full example configuration files [here](https://github.com/l0gr1thm1k/data-generated-cube/tree/github/src/cube_config/example_configs). Here is a table breaking down each
key in the configuration file:

| Key | Description | Example | Notes                                                                                                                                                                                                           |
| --- | --- | --- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| cubeName | The name of the cube you are generating. | example_data_generated_cube | any string will do                                                                                                                                                                                              |
| cardBlacklist | A list of card names that you do not want to include in the cube. | ["Black Lotus", "Mox Pearl"] | Can be null or a list of string values                                                                                                                                                                          |
| cardCount | The number of cards you want in the cube. | 360 | This will generate the cube at the target size but you may still sample cubes at +/- 10% of this size                                                                                                           |
| cubeCategory | The cube category you want to generate. | Vintage | This is the cube category you want to generate. Options are Vintage, Powered, Unpowered, Pauper, Peasant, Budget, Silver-bordered, Commander, Battle Box, Multiplayer, Judge Tower                              |
| cubeIds | A list of cube IDs from Cube Cobra that you want to include in the cube. | ["modovintage", "wtwlf123", "synergy", "LSVCubeInit", "AlphaFrog"] | This is a list of cube IDs from Cube Cobra that you want to include in the cube. It can be the shortID which are generally human readable or the long IDs, which are GUID values.                               |
| overwrite | A boolean value indicating whether you want to overwrite the cube if it already exists. | true | If true, the cube will be overwritten if it already exists. If false, the cube will not be overwritten if it already exists.                                                                                    |
| stages | A list of stages you want to run in the pipeline. | ["scrape", "create", "analyze"] | This is a list of <br/>stages you want to run in the pipeline. Options are scrape, create, and analyze. You can skip 'scrape' for example if you just want to regenerate the cube with previously crawled data. |
| useCubeCobraBucket | A boolean value indicating whether you want to use the Cube Cobra bucket. | true | If true, the Cube Cobra bucket will be used. If false, the Cube Cobra bucket will not be used.                                                                                                                  |

#### Using the Cube Cobra Bucket
The Cube Cobra bucket is a bucket in the Cube Cobra S3 bucket that contains all the cube data. Ths project uses the 
bucket data to streamline the process of gathering cubes to sample for the data generated cube. The bucket requires two 
variables to be set in your environment. 

1. [CUBE_COBRA_AWS_ACCESS_KEY_ID](https://github.com/l0gr1thm1k/data-generated-cube/blob/f1fb9e6aa0513ea03ffa2800c57083081b44a9df/src/common/constants.py#L59)
2. [CUBE_COBRA_AWS_SECRET_ACCESS_KEY](https://github.com/l0gr1thm1k/data-generated-cube/blob/f1fb9e6aa0513ea03ffa2800c57083081b44a9df/src/common/constants.py#L60)

You will need to contact the admin of Cube Cobra [Gwen Dekker](https://github.com/dekkerglen) in order to get your own access keys if you would like 
to use the AWS data. For a quicker result, I recommend setting this boolean value to false and supplying your own 
list of cube IDs in the configuration file.

### Running the Pipeline
To run the pipeline, you will need to run the `__main__.py` script with the path to your configuration JSON as in 
the [__main__.py](https://github.com/l0gr1thm1k/data-generated-cube/blob/github/__main__.py#L13) file. 

1. Navigate to the project directory. Again this depends on where you cloned the repository.:

    ```sh
    cd data-generated-cube
    ```

2. Update `__main__.py` to include the path to your configuration file. 

    ```python
    if __name__ == "__main__":
        main("path/to/your/configuration/file.json")
    ```
   
3. Run the `__main__.py` script. 

    ```sh
    python __main__.py
    ```


## Support
1. If you have questions related to this pipeline you can reach out to me here on GitHub.
2. Questions related to Cube Cobra can be directed to the Cube Cobra [Discord](https://discord.gg/FpXmMhkb)
3. Questions related to Scryfall can be directed to the Scryfall [GitHub](https://github.com/scryfall)

## Thanks
Many thanks to all the folks in the cube community who have made suggestions and improvements over the years to this 
pipeline. Special thanks to [Gwen Dekker](https://github.com/dekkerglen) for access to the data and for his help in 
providing guidance on how to use the Cube Cobra bucket. Thanks to [Keldan Campbell](https://twitter.com/CampbellKeldan) 
for suggesting updates to the sampling process based on cube frequency and recency of updates. 

