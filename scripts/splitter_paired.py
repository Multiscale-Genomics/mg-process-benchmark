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

from TaskWrappers.fastq_split import ProcessSplitFastQPaired

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=16384"
MEMORY_FLAG_ALIGNMENT = "16384"
RESOURCE_FLAG_MERGE = "mem=16384"
MEMORY_FLAG_MERGE = "16384"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False

FASTQ_CHUNK_SIZE = 1000000

class FastQSplitterPaired(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    in_fastq_file_1 = luigi.Parameter()
    in_fastq_file_2 = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        fq_tmp = self.in_fastq_file_1.split("/")
        fq_tmp[-1] = fq_tmp[-1].replace(".fastq", ".tmp_0.fastq")
        return luigi.LocalTarget("/".join(fq_tmp))

    def run(self):
        """
        Worker function for aligning single ended FASTQ reads using BWA ALN

        Parameters
        ----------
        in_fastq_file_1 : str
            Location of the FASTQ file
        in_fastq_file_2 : str
            Location of the FASTQ file
        """
        print("### PROCESSING SPLITTER")
        split_fastq = ProcessSplitFastQPaired(
            in_fastq_file_1=self.in_fastq_file_1, in_fastq_file_2=self.in_fastq_file_2,
            fastq_chunk_size=FASTQ_CHUNK_SIZE,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            save_job_info=SAVE_JOB_INFO, extra_bsub_args=self.user_python_path)
        yield split_fastq


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="Split Single Ended FASTQ Pipeline Wrapper")
    PARSER.add_argument("--in_fastq_file_1", help="")
    PARSER.add_argument("--in_fastq_file_2", help="")
    PARSER.add_argument("--fastq_chunk_size", default=1000000, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir
    FASTQ_CHUNK_SIZE = ARGS.fastq_chunk_size

    luigi.build(
        [
            FastQSplitterPaired(
                in_fastq_file_1=ARGS.in_fastq_file_1,
                in_fastq_file_2=ARGS.in_fastq_file_2,
                user_python_path=ARGS.python_path
            )
        ],
        local_scheduler=True, workers=50)
