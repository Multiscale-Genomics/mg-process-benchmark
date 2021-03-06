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
from TaskWrappers.bwa_aln import ProcessAlnBwaPaired
from TaskWrappers.bam_merge import ProcessMergeBams


SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=16384"
MEMORY_FLAG_ALIGNMENT = "16384"
RESOURCE_FLAG_MERGE = "mem=16384"
MEMORY_FLAG_MERGE = "16384"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False

FASTQ_CHUNK_SIZE = 1000000

class BwaAlnPaired(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
    """

    genome_fa = luigi.Parameter()
    genome_idx = luigi.Parameter()
    in_fastq_file_1 = luigi.Parameter()
    in_fastq_file_2 = luigi.Parameter()
    raw_bam_file = luigi.Parameter()
    batch_size = luigi.IntParameter()
    user_python_path = luigi.Parameter()

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
        Worker function for aligning single ended FASTQ reads using BWA ALN

        Parameters
        ----------
        genome_fa : str
            Location of the FASTA file of the genome to align the reads to
        genome_idx : str
            Location of the index files in .tar.gz file prepared by the BWA
            indexer
        in_fastq_file_1 : str
            Location of the FASTQ file
        in_fastq_file_2 : str
            Location of the FASTQ file
        raw_bam_file : str
            Location of the aligned reads in bam format
        """
        print("### PROCESSING SPLITTER")
        split_fastq = ProcessSplitFastQPaired(
            in_fastq_file_1=self.in_fastq_file_1, in_fastq_file_2=self.in_fastq_file_2,
            fastq_chunk_size=FASTQ_CHUNK_SIZE,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            job_name_flag="splitter", save_job_info=SAVE_JOB_INFO,
            extra_bsub_args=self.user_python_path)
        yield split_fastq

        outfiles = []
        with open(split_fastq.output().path, "r") as fastq_sub_files:
            for fastq_sub_file in fastq_sub_files:
                outfiles.append(fastq_sub_file.strip().split("\t"))
        output_alignments = []
        alignment_jobs = []
        for fastq_file in outfiles:
            output_bam = fastq_file[0].replace(".fastq", ".bam")
            job_name = fastq_file[0].split("/")
            alignment = ProcessAlnBwaPaired(
                genome_fa=self.genome_fa,
                genome_idx=self.genome_idx,
                fastq_file_1=fastq_file[0],
                fastq_file_2=fastq_file[1],
                output_bam=output_bam,
                n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
                resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
                queue_flag=QUEUE_FLAG, job_name_flag=job_name[-1], save_job_info=SAVE_JOB_INFO,
                extra_bsub_args=self.user_python_path)
            output_alignments.append(alignment.output().path)
            alignment_jobs.append(alignment)
        yield alignment_jobs

        merged_alignment = ProcessMergeBams(
            bam_files=",".join(output_alignments),
            bam_file_out=self.raw_bam_file,
            batch_size=self.batch_size,
            user_shared_tmp_dir=SHARED_TMP_DIR,
            user_queue_flag=QUEUE_FLAG,
            user_save_job_info=SAVE_JOB_INFO,
            user_resource_flag=RESOURCE_FLAG_MERGE,
            user_memory_flag=MEMORY_FLAG_MERGE,
            user_python_path=self.user_python_path
        )
        yield merged_alignment

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="BWA ALN Paired End Pipeline Wrapper")
    PARSER.add_argument("--genome_fa", help="")
    PARSER.add_argument("--genome_idx", help="")
    PARSER.add_argument("--in_fastq_file_1", help="")
    PARSER.add_argument("--in_fastq_file_2", help="")
    PARSER.add_argument("--raw_bam_file", help="")
    PARSER.add_argument("--batch_bam_size", default=10, help="")
    PARSER.add_argument("--fastq_chunk_size", default=1000000, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")
    PARSER.add_argument("--python_path", default=sys.executable, help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir
    FASTQ_CHUNK_SIZE = ARGS.fastq_chunk_size

    luigi.build(
        [
            BwaAlnPaired(
                genome_fa=ARGS.genome_fa,
                genome_idx=ARGS.genome_idx,
                in_fastq_file_1=ARGS.in_fastq_file_1,
                in_fastq_file_2=ARGS.in_fastq_file_2,
                raw_bam_file=ARGS.raw_bam_file,
                batch_size=ARGS.batch_bam_size,
                user_python_path=ARGS.python_path
            )
        ],
        local_scheduler=True, workers=250)
