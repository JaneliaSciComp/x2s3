#!/usr/bin/env python
""" Run a development server using Uvicorn.
"""

import argparse
import uvicorn
from jproxy.app import app

if __name__ == "__main__":

    argparser = argparse.ArgumentParser(description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    argparser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8000,
        help="port number for API",
    )
    args = argparser.parse_args()

    uvicorn.run(app, host='0.0.0.0', port=args.port, workers=1)

