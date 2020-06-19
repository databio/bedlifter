#!/usr/bin/env python3
"""
liftover bed files produced by bedmaker to hg18, hg19, and hg38
"""
__author__ = ["Jose Verdezoto"]
__email__ = "jev4xy@virginia.edu"
__version__ = "0.0.1-dev"

from argparse import ArgumentParser
import pypiper
import os
import sys
import gzip
import shutil


parser = ArgumentParser(description="A pipeline to liftOver bedmaker outputs to hg18, hg19 and hg38 assemblies")

parser.add_argument("-b", "--bedfile", help=" path to the BED file to be lifted over, doesn't need to be gzipped", type=str) 
parser.add_argument("-g", "--genome", type=str, required=True, help="genome assembly of the file to be lifted over")
parser.add_argument("-c", "--chain-files", action="store", type=str, nargs="*",
                    help="path to the chain file(s) ffacilitating conversion from one assembly to the other")
parser.add_argument("-f", "--outfolder", type=str, required=True, help="path to folder where pipeline logs and lifted files will be stored")


# add pypiper args 
parser = pypiper.add_pypiper_args(parser, groups=["pypiper"], required=["--bedfile", "--genome"])
args = parser.parse_args()

# Set output folder
logs_name = "bedlifter_logs"
logs_dir = os.path.join(args.outfolder, logs_name)

if not os.path.exists(logs_dir):
	print("bedlifter logs directory doesn't exist. Creating one...")
	os.makedirs(logs_dir)


def main():
    pm = pypiper.PipelineManager(name="bedlifter", outfolder=logs_dir, args=args)

    # Define id for new bed file
    bed_file = os.path.basename(args.bedfile)
    bed_id = bed_file.split(".")[0]
	

	# Parse through list of chain files and extract the starting genome to see if matches the bed file assembly
	# Need to gunzip chain files if user provides compressed files
    
    chain_list = [] # chain list is a list with the paths to each unzipped chain file
    chain_starting_genomes = []
    chain_target_genomes = []
    for i in args.chain_files:
        chain_parent = os.path.dirname(i)
        chain_name = os.path.basename(i)
        start_assembly = chain_name.split("T")[0]
        chain_starting_genomes.append(start_assembly) # list with the names of starting genome assemblies
        target_assembly = chain_name.split("To", 1)[1].split(".over", 1)[0]
        chain_target_genomes.append(target_assembly) # This target assembly string will be used specifically to name the lifted files
        chain_extension = os.path.splitext(chain_name)[1]   
    	# Use the gzip module to produce temporary unzipped files
        if chain_extension == ".gz":
            chain_new = os.path.splitext(chain_name)[0]
            chain_new_path = os.path.abspath(os.path.join(chain_parent, chain_new))
            with gzip.open(i, "rb") as f_in:
                with open(chain_new_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            chain_list.append(chain_new_path)
            pm.clean_add(chain_new_path)
        else:
            chain_list.append(i)

    print("This is my chain list", chain_list)
    # Command templates
    # liftOver template
    liftOver_template = "liftOver {old_bed_file} {chain_file} {new_bed_file} unMapped.txt -bedPlus=3"
    # gzip converted files
    gzip_template = "gzip -k {converted_bed}"

 

	# Need to parse through chain_files argument and exctract the name of the starting genomes

    for i,j,k in zip(chain_starting_genomes, chain_list, chain_target_genomes):
        if args.genome == i: 
            print("starting_genome", i)
            new_bed_file_path = os.path.abspath(os.path.join(args.outfolder, bed_id + "_" + i + "to" + k + ".bed"))
            cmd1 = liftOver_template.format(old_bed_file = args.bedfile, chain_file=j, new_bed_file= new_bed_file_path)
            cmd2 = gzip_template.format(converted_bed=new_bed_file_path)
            cmd = [cmd1, cmd2]
            pm.run(cmd, target=new_bed_file_path)
            pm.clean_add(new_bed_file_path)
    
    pm.stop_pipeline()


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("Pipeline aborted.")
        sys.exit(1)





