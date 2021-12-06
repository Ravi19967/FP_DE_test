## Instructions to Run
Prequistives: Python37, Docker and Shell
* To ensure it runs change directory to package base
* For First time run you will have to build the docker image `build_docker.sh`
* Once the image is build it will automatically trigger the run and dump the results to the same directory
* After first run to trigger the code you can directly run `run.sh`

## Adding new Files
* To update the input files you can update the `data.py` file for input variables. Currently they are fetching input directly from github repo
`https://github.com/k-edge/data_engineering_challenge_fp` 
*If you want to add input files locally you will have to update the docker file to copy the input files and also update the read function in the JSON Reader class accordingly.

## Design Considerations
* Json loading happens in memory so large json files can be a problem with memory
* Pydantic ensures schema and type of value remains consistent. It won't break if new key-value pair are added but only changing type of present values
* Due to the dynamic nature of time series to calculate correlation. I had to interpolate the data
* An assumption that should hold in reality is we won't see data before 1st Jan 1970