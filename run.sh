docker stop RavijitR_assignment && docker rm -f RavijitR_assignment
docker run --name=RavijitR_assignment fp_de:1.0 
docker cp RavijitR_assignment:/output_data/ .