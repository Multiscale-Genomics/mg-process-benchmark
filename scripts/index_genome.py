"""
.. See the NOTICE file distributed with this work for additional information
   regarding copyright ownership.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from __future__ import print_function

import argparse

import luigi

from TaskWrappers.bowtie2_index import ProcessIndexBowtie2
from TaskWrappers.bwa_index import ProcessIndexBwa
from TaskWrappers.gem_index import ProcessIndexGem


SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=8192"
MEMORY_FLAG_ALIGNMENT = "8192"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False

FASTQ_CHUNK_SIZE = 1000000

class BwaAlnSingle(luigi.Task):
    """
    Pipeline for aligning single end reads using Bowtie 2
    """

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    in_fastq_file = luigi.Parameter()
    raw_bam_file = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return luigi.LocalTarget(self.raw_bam_file)

    def run(self):
        """
        Worker function for aligning single ended FASTQ reads using Bowtie2

        Parameters
        ----------
        genome_fa : str
            Location of the FASTA file of the genome to align the reads to
        genome_idx : str
            Location of the index files in .tar.gz file prepared by the BWA
            indexer
        in_fastq_file : str
            Location of the FASTQ file
        raw_bam_file : str
            Location of the aligned reads in bam format
        """
        index_jobs = []
        index = ProcessIndexBowtie2(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".bwt.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO)
        index_jobs.append(index)

        index = ProcessIndexBwa(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".bwa.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO)
        index_jobs.append(index)

        index = ProcessIndexGem(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".gem.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO)
        index_jobs.append(index)

        yield index_jobs

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="Bowtie2 Single Ended Pipeline Wrapper")
    PARSER.add_argument("--genome_fa", help="")
    PARSER.add_argument("--shared_tmp_dir", help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir
    FASTQ_CHUNK_SIZE = ARGS.fastq_chunk_size

    luigi.build(
        [
            BwaAlnSingle(
                genome_fa=ARGS.genome_fa
            )
        ],
        local_scheduler=True, workers=250)
