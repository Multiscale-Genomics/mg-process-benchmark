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

from TaskWrappers.kallisto import ProcessKallistoSingle, ProcessKallistoPaired

SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=16384"
MEMORY_FLAG_ALIGNMENT = "16384"
RESOURCE_FLAG_MERGE = "mem=16384"
MEMORY_FLAG_MERGE = "16384"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False


class kallisto_single(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    in_fastq_file_1 = luigi.Parameter()
    genome_file = luigi.Parameter()
    index_file = luigi.Parameter()
    abundance = luigi.Parameter()
    abundance_h5 = luigi.Parameter()
    run_info = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return [
            luigi.LocalTarget(self.index_file),
            luigi.LocalTarget(self.abundance),
            luigi.LocalTarget(self.abundance_h5),
            luigi.LocalTarget(self.run_info)
        ]

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
        kallisto_handle = ProcessKallistoSingle(
            in_fastq_file_1=self.in_fastq_file_1,
            genome_file=self.genome_file,
            index_file=self.index_file,
            abundance=self.abundance,
            abundance_h5=self.abundance_h5,
            run_info=self.run_info,
            user_python_path=self.user_python_path,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            job_name_flag="kallisto", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield kallisto_handle


class kallisto_paired(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    in_fastq_file_1 = luigi.Parameter()
    in_fastq_file_2 = luigi.Parameter()
    genome_file = luigi.Parameter()
    index_file = luigi.Parameter()
    abundance = luigi.Parameter()
    abundance_h5 = luigi.Parameter()
    run_info = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return [
            luigi.LocalTarget(self.index_file),
            luigi.LocalTarget(self.abundance),
            luigi.LocalTarget(self.abundance_h5),
            luigi.LocalTarget(self.run_info)
        ]

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
        kallisto_handle = ProcessKallistoPaired(
            in_fastq_file_1=self.in_fastq_file_1,
            in_fastq_file_2=self.in_fastq_file_2,
            genome_file=self.genome_file,
            index_file=self.index_file,
            abundance=self.abundance,
            abundance_h5=self.abundance_h5,
            run_info=self.run_info,
            user_python_path=self.user_python_path,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            job_name_flag="kallisto", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield kallisto_handle


if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="BioBamBam Pipeline Wrapper")
    PARSER.add_argument("--in_fastq_file_1", help="")
    PARSER.add_argument("--in_fastq_file_2", default=None, help="")
    PARSER.add_argument("--genome_file", help="")
    PARSER.add_argument("--index_file", help="")
    PARSER.add_argument("--abundance", help="")
    PARSER.add_argument("--abundance_h5", help="")
    PARSER.add_argument("--run_info", help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir

    if ARGS.in_fastq_file_2 is not None:
        luigi.build(
            [
                kallisto_paired(
                    in_fastq_file_1=ARGS.in_fastq_file_1,
                    in_fastq_file_2=ARGS.in_fastq_file_2,
                    genome_file=ARGS.genome_file,
                    index_file=ARGS.index_file,
                    abundance=ARGS.abundance,
                    abundance_h5=ARGS.abundance_h5,
                    run_info=ARGS.run_info,
                    user_python_path=ARGS.python_path
                )
            ],
            local_scheduler=True, workers=5)
    else:
        luigi.build(
            [
                kallisto_single(
                    in_fastq_file_1=ARGS.in_fastq_file_1,
                    genome_file=ARGS.genome_file,
                    index_file=ARGS.index_file,
                    abundance=ARGS.abundance,
                    abundance_h5=ARGS.abundance_h5,
                    run_info=ARGS.run_info,
                    user_python_path=ARGS.python_path
                )
            ],
            local_scheduler=True, workers=5)
