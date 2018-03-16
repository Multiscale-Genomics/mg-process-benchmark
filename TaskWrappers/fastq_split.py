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

# import logging
import luigi

from luigi.contrib.lsf import LSFJobTask

from tool.fastq_splitter import fastq_splitter

class ProcessSplitFastQSingle(LSFJobTask):
    """
    Tool wrapper for splitting FASTQ files into a defined chunk size.
    """

    in_fastq_file = luigi.Parameter()
    fastq_chunk_size = luigi.IntParameter()
    out_fastq_files = []

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the fastq directory. List of the generated fastq files
        """
        root_name = self.in_fastq_file.split("/")
        return luigi.LocalTarget("/".join(root_name[0:-1]) + "/tmp/fastq_file_log.txt")

    def work(self):
        """
        Worker function for splitting the FASTQ file into smaller chunks

        Parameters
        ----------
        in_fastq_file : str
            Location of the FASTQ file to split
        fastq_chunk_size : int
            Number of reads that each FASTQ chunk should contain
        """
        fqs = fastq_splitter({
            "fastq_chunk_size" : self.fastq_chunk_size,
            "no-untar" : True
        })
        results = fqs.single_splitter(
            self.in_fastq_file, self.in_fastq_file + ".tar.gz")

        root_name = self.in_fastq_file.split("/")
        with open("/".join(root_name[0:-1]) + "/tmp/fastq_file_log.txt", "w") as f_out:
            for fastq_file in results:
                f_out.write("/".join(root_name[0:-1]) + "/tmp/" + fastq_file[0] + "\n")

class ProcessSplitFastQPaired(LSFJobTask):
    """
    Tool wrapper for splitting FASTQ files into a defined chunk size.
    """

    in_fastq_file_1 = luigi.Parameter()
    in_fastq_file_2 = luigi.Parameter()
    fastq_chunk_size = luigi.IntParameter()
    out_fastq_files = []

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the fastq directory. List of the generated fastq files
        """
        root_name = self.in_fastq_file_1.split("/")
        return luigi.LocalTarget("/".join(root_name[0:-1]) + "/tmp/fastq_file_log.txt")

    def work(self):
        """
        Worker function for splitting the FASTQ file into smaller chunks

        Parameters
        ----------
        in_fastq_file_1 : str
            Location of the FASTQ file to split
        in_fastq_file_2 : str
            Location of the FASTQ file to split
        fastq_chunk_size : int
            Number of reads that each FASTQ chunk should contain
        """
        fqs = fastq_splitter({
            "fastq_chunk_size" : self.fastq_chunk_size,
            "no-untar" : True
        })
        results = fqs.paired_splitter(
            self.in_fastq_file_1, self.in_fastq_file_2, self.in_fastq_file_1 + ".tar.gz")

        root_name = self.in_fastq_file_1.split("/")
        with open("/".join(root_name[0:-1]) + "/tmp/fastq_file_log.txt", "w") as f_out:
            for fastq_file in results:
                file_1 = "/".join(root_name[0:-1]) + "/tmp/" + fastq_file[0]
                file_2 = "/".join(root_name[0:-1]) + "/tmp/" + fastq_file[1]
                f_out.write(file_1 + "\t" + file_2 + "\n")
