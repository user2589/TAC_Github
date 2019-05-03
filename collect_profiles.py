#!/usr/bin/env python3

"""Collect contributor profiles of selected repositories"""


import utils
import logging


def collect_profiles(repo_slugs):
    for repo_slug in repo_slugs:
        logging.info(repo_slug)
        try:
            assignees = utils.get_assignees(repo_slug)
        except KeyboardInterrupt:
            raise
        except:
            logging.warning("Failed to get assignees for %s", repo_slug)
            continue

        for assignee in assignees.index:
            logging.info("\t%s", assignee)
            try:
                profile = utils.get_profile(assignee)
            except KeyboardInterrupt:
                raise
            except:
                logging.warning("\tFailed to get profile for %s", assignee)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s',
                        level=logging.INFO)

    collect_profiles(utils.get_all_packages())
