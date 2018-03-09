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

import sys
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
RUN_TIME_FLAG = 600

class IndexGenome(luigi.Task):
    """
    Pipeline for indexing genomes provided in FASTA format
    """

    genome_fa = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : list
            luigi.LocalTarget() - Location of the Bowtie2 index
            luigi.LocalTarget() - Location of the BWA index
            luigi.LocalTarget() - Location of the GEM formatted FASTA file
            luigi.LocalTarget() - Location of the GEM index
        """
        return [
            luigi.LocalTarget(self.genome_fa + ".bt2.tar.gz"),
            luigi.LocalTarget(self.genome_fa + ".bwa.tar.gz"),
            luigi.LocalTarget(self.genome_fa.replace(".fasta", ".gem.fasta")),
            luigi.LocalTarget(self.genome_fa + ".gem.tar.gz")
        ]

    def run(self):
        """
        Worker function for generating indexes for a given genomein FASTA format

        Parameters
        ----------
        genome_fa : str
            Location of the FASTA file of the genome to generate indexes for
        """
        index_jobs = []
        index = ProcessIndexBowtie2(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".bt2.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO, runtime_flag=RUN_TIME_FLAG,
            extra_bsub_args=self.user_python_path)
        index_jobs.append(index)

        index = ProcessIndexBwa(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".bwa.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO, runtime_flag=RUN_TIME_FLAG,
            extra_bsub_args=self.user_python_path)
        index_jobs.append(index)

        index = ProcessIndexGem(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_fa + ".gem.tar.gz",
            n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
            resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
            queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO, runtime_flag=RUN_TIME_FLAG,
            extra_bsub_args=self.user_python_path)
        index_jobs.append(index)

        yield index_jobs

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="Genome indexer")
    PARSER.add_argument("--genome_fa", help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    luigi.build(
        [
            IndexGenome(
                genome_fa=ARGS.genome_fa,
                user_python_path=ARGS.python_path
            )
        ],
        local_scheduler=True, workers=250)
