from setuptools import setup, find_packages

setup(
    name="pgs-tools",
    version="0.1.0",
    packages=find_packages(include=['lib', 'tools']),
    install_requires=[
        "psycopg2",
        "sqlparse",
    ],
    entry_points={
        'console_scripts': [
            'pgs-bench=tools.pgs_bench:main',
            'pgs-ps=tools.pgs_ps:main',
            'pgs-info=tools.pgs_info:main',
            'pgs-stat=tools.pgs_stat:main',
            'pgs-top=tools.pgs_top:main',
            'pgs-vacuum=tools.pgs_vacuum:main',
            'pgs-warmupper=tools.pgs_warmupper:main',
        ],
    },
    author="Perfguru87",
    author_email="perfguru87@gmail.com",
    description="PostgreSQL System management and monitoring tools",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/perfguru87/pgs-tools",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License 2.0",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
