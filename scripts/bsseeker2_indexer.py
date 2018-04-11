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

from TaskWrappers.bss2_indexer import ProcessBSSeekerIndexer

SHARED_TMP_DIR = ""
RESOURCE_FLAG = "mem=32768"
MEMORY_FLAG = "32768"
RUNTIME_FLAG = 600
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class BSseeker2Indexer(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    aligner = luigi.Parameter()
    aligner_path = luigi.Parameter()
    bss_path = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return luigi.LocalTarget(self.genome_idx)

    def run(self):
        """
        Worker function for aligning single ended FASTQ reads using BWA ALN

        Parameters
        ----------
        in_bam_file : str
            Location of the bam file
        out_bam_file : str
            Location of the filtered bam file
        """
        print("### PROCESSING BAM FILE")
        bss2_handle = ProcessBSSeekerIndexer(
            genome_fa=self.genome_fa,
            genome_idx=self.genome_idx,
            aligner=self.aligner,
            aligner_path=self.aligner_path,
            bss_path=self.bss_path,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            resource_flag=RESOURCE_FLAG, memory_flag=MEMORY_FLAG,
            runtime_flag=RUNTIME_FLAG,
            job_name_flag="BSS2 IDX", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield bss2_handle


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="BS Seeker 2 Peak Caller Pipeline Wrapper")
    PARSER.add_argument("--genome_fa", help="")
    PARSER.add_argument("--genome_idx", help="")
    PARSER.add_argument("--aligner", help="")
    PARSER.add_argument("--aligner_path", help="")
    PARSER.add_argument("--bss_path", help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    luigi.build(
        [
            BSseeker2Indexer(
                genome_fa=ARGS.genome_fa,
                genome_idx=ARGS.genome_idx,
                aligner=ARGS.aligner,
                aligner_path=ARGS.aligner_path,
                bss_path=ARGS.bss_path,
                user_python_path=ARGS.python_path
            )
        ],
        local_scheduler=True, workers=5)
