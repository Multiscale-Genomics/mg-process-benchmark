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

from TaskWrappers.fastq_split import ProcessSplitFastQSingle
from TaskWrappers.bwa_aln import ProcessAlnBwaSingle
from TaskWrappers.bam_merge import ProcessMergeBams


SHARED_TMP_DIR = ""
RESOURCE_FLAG_ALIGNMENT = "mem=8192"
MEMORY_FLAG_ALIGNMENT = "8192"
QUEUE_FLAG = "production-rh7"
SAVE_JOB_INFO = False

FASTQ_CHUNK_SIZE=1000000

class BwaAlnSingle(luigi.Task):
    """
    Pipeline for aligning single end reads using BWA ALN
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
        Worker function for splitting the FASTQ file into smaller chunks

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
        split_fastq = ProcessSplitFastQSingle(
            in_fastq_file=self.in_fastq_file, fastq_chunk_size=FASTQ_CHUNK_SIZE,
            n_cpu_flag=1, shared_tmp_dir=SHARED_TMP_DIR, queue_flag=QUEUE_FLAG,
            save_job_info=SAVE_JOB_INFO)
        yield split_fastq

        print("INPUT OBJECT", split_fastq.output())
        outfiles = []

        with open(split_fastq.output().path, "r") as fastq_sub_files:
            for fastq_sub_file in fastq_sub_files:
                outfiles.append(fastq_sub_file.strip())

        output_alignments = []
        alignment_jobs = []
        for fastq_file in outfiles:
            output_bam = fastq_file.replace(".fastq", ".bam")
            alignment = ProcessAlnBwaSingle(
                genome_fa=self.genome_fa,
                genome_idx=self.genome_idx,
                fastq_file=fastq_file,
                output_bam=output_bam,
                n_cpu_flag=5, shared_tmp_dir=SHARED_TMP_DIR,
                resource_flag=RESOURCE_FLAG_ALIGNMENT, memory_flag=MEMORY_FLAG_ALIGNMENT,
                queue_flag=QUEUE_FLAG, save_job_info=SAVE_JOB_INFO)
            output_alignments.append(alignment.output().path)
            alignment_jobs.append(alignment)
        yield alignment_jobs

        merged_alignment = ProcessMergeBams(
            bam_files=",".join(output_alignments),
            bam_file_out=self.raw_bam_file,
            user_shared_tmp_dir=SHARED_TMP_DIR,
            user_queue_flag=QUEUE_FLAG,
            user_save_job_info=SAVE_JOB_INFO
        )
        yield merged_alignment

if __name__ == "__main__":
    # Set up the command line parameters
    PARSER = argparse.ArgumentParser(description="BWA ALN Single Ended Pipeline Wrapper")
    PARSER.add_argument("--genome_fa", help="")
    PARSER.add_argument("--genome_idx", help="")
    PARSER.add_argument("--in_fastq_file", help="")
    PARSER.add_argument("--raw_bam_file", help="")
    PARSER.add_argument("--fastq_chunk_size", default=1000000, help="")
    PARSER.add_argument("--shared_tmp_dir", help="")

    # Get the matching parameters from the command line
    ARGS = PARSER.parse_args()

    SHARED_TMP_DIR = ARGS.shared_tmp_dir
    FASTQ_CHUNK_SIZE = ARGS.fastq_chunk_size

    luigi.build(
        [
            BwaAlnSingle(
                genome_fa=ARGS.genome_fa,
                genome_idx=ARGS.genome_idx,
                in_fastq_file=ARGS.in_fastq_file,
                raw_bam_file=ARGS.raw_bam_file,
            )
        ],
        local_scheduler=True, workers=100)
