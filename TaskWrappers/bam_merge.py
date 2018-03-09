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

from tool.bam_utils import bamUtils

# logger = logging.getLogger('luigi-interface')

class TimeTaskMixin(object):  # pylint: disable=too-few-public-methods
    """
    Timer object
    """

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def print_execution_time(self, processing_time):  # pylint: disable=no-self-use
        """
        Print the length of time the task ran for (seconds)
        """
        print('### PROCESSING TIME - Merging BAMs ###: ' + str(processing_time))

class ProcessMergeBamsJob(LSFJobTask, TimeTaskMixin):
    """
    Tool wrapper for merging the bam files on the LSF cluster
    """

    bam_files = luigi.Parameter()
    bam_file_out = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return luigi.LocalTarget(self.bam_file_out)

    def work(self):
        """
        Tool worker to merge multiple bam files into a single bam file to run on
        the LSF cluster

        Parameters
        ----------
        bam_files : str
            Comma separated string of the list of bam files
        bam_file_out : str
            Location of the output bam file
        """
        bam_job_files = self.bam_files.split(",")

        bam_handle = bamUtils()
        bam_handle.bam_copy(bam_job_files.pop(0), self.bam_file_out)
        bam_handle.bam_merge([self.bam_file_out] + bam_job_files)
        # bam_handle.bam_copy(bam_job_files[0], self.bam_file_out)

class ProcessSortBamJob(LSFJobTask, TimeTaskMixin):
    """
    Tool wrapper for merging the bam files on the LSF cluster
    """

    bam_file = luigi.Parameter()
    bam_file_out = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return luigi.LocalTarget(self.bam_file_out)

    def work(self):
        """
        Tool worker to merge multiple bam files into a single bam file to run on
        the LSF cluster

        Parameters
        ----------
        bam_files : str
            Comma separated string of the list of bam files
        bam_file_out : str
            Location of the output bam file
        """
        bam_handle = bamUtils()
        bam_handle.bam_sort(self.bam_file)
        bam_handle.bam_copy(self.bam_file, self.bam_file_out)

class ProcessMergeBams(luigi.Task):
    """
    Tool wrapper for the management of merging multiple bam files into a single
    bam file.
    """

    bam_files = luigi.Parameter()
    bam_file_out = luigi.Parameter()
    batch_size = luigi.IntParameter()
    user_shared_tmp_dir = luigi.Parameter()
    user_queue_flag = luigi.Parameter()
    user_save_job_info = luigi.Parameter()
    user_resource_flag = luigi.Parameter()
    user_memory_flag = luigi.Parameter()
    user_python_path = luigi.Parameter()

    def output(self):
        """
        Returns
        -------
        output : luigi.LocalTarget()
            Location of the merged aligned reads in bam format
        """
        return luigi.LocalTarget(self.bam_file_out)

    def run(self):
        """
        Tool worker to merge multiple bam files into a single bam file to run on
        the LSF cluster

        Parameters
        ----------
        bam_files : str
            Comma separated string of the list of sorted bam files
        bam_file_out : str
            Location of the output sorted bam file
        """
        bam_job_files = self.bam_files.split(",")

        merge_round = -1
        while True:
            print("FILES TO MERGE:", bam_job_files)
            merge_round += 1
            if len(bam_job_files) > 1:
                tmp_alignments = []
                merge_jobs = []

                current_list_len = len(bam_job_files)
                for i in range(0, current_list_len-(self.batch_size-1), self.batch_size):  # pylint: disable=unused-variable
                    bam_job_array = []
                    for j in range(self.batch_size):  # pylint: disable=unused-variable
                        bam_job_array.append(bam_job_files.pop(0))

                    if merge_round == 0:
                        bam_out = bam_job_array[0].replace(
                            ".bam", "_merge_" + str(merge_round) + ".bam")
                    else:
                        bam_out = bam_job_array[0].replace(
                            "_merge_" + str(merge_round - 1) + ".bam",
                            "_merge_" + str(merge_round) + ".bam")

                    merge_job = ProcessMergeBamsJob(
                        bam_files=",".join(bam_job_array),
                        bam_file_out=bam_out,
                        n_cpu_flag=1, shared_tmp_dir=self.user_shared_tmp_dir,
                        resource_flag=self.user_resource_flag, memory_flag=self.user_memory_flag,
                        queue_flag=self.user_queue_flag, job_name_flag="bam_merger",
                        save_job_info=self.user_save_job_info,
                        extra_bsub_args=self.user_python_path
                    )
                    tmp_alignments.append(bam_out)
                    merge_jobs.append(merge_job)

                if bam_job_files:
                    if merge_round == 0:
                        bam_out = bam_job_files[0].replace(
                            ".bam", "_merge_" + str(merge_round) + ".bam")
                    else:
                        bam_out = bam_job_files[0].replace(
                            "_merge_" + str(merge_round - 1) + ".bam",
                            "_merge_" + str(merge_round) + ".bam")

                    merge_job = ProcessMergeBamsJob(
                        bam_files=",".join(bam_job_files),
                        bam_file_out=bam_out,
                        n_cpu_flag=1, shared_tmp_dir=self.user_shared_tmp_dir,
                        resource_flag=self.user_resource_flag, memory_flag=self.user_memory_flag,
                        queue_flag=self.user_queue_flag, job_name_flag="bam_merger",
                        save_job_info=self.user_save_job_info,
                        extra_bsub_args=self.user_python_path
                    )
                    tmp_alignments.append(bam_out)
                    merge_jobs.append(merge_job)

                yield merge_jobs

                bam_job_files = []
                bam_job_files = [new_bam for new_bam in tmp_alignments]

            else:
                break

        sort_job = ProcessSortBamJob(
            bam_file=bam_job_files.pop(0),
            bam_file_out=self.bam_file_out,
            n_cpu_flag=1, shared_tmp_dir=self.user_shared_tmp_dir,
            queue_flag=self.user_queue_flag, save_job_info=self.user_save_job_info,
            extra_bsub_args=self.user_python_path
        )
        yield sort_job
