#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
##
# Description:-
#
#       Starter Script to read, process and persist data, as part of
#       recruitment process at Miro for Senior Data Engineer
##
# Development date    Developed by       Comments
# ----------------    ------------       ---------
# 12/10/2021          Saddam Khan        Initial version
#
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

from Miro_Task.Miro.jobs.miro_elt_main import *

if __name__ == '__main__':
    """
        Entry point for Miro ETL Job
    """
    main()