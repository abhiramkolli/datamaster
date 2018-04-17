# datamaster
#Export Script
conda env export -n datamaster > environment.yml
#Create Script
conda env create -f environment.yml
