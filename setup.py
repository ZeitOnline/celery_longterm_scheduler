from setuptools import setup, find_packages


setup(
    name='celery_longterm_scheduler',
    version='1.2.0',
    author='Zeit Online',
    author_email='zon-backend@zeit.de',
    url='https://github.com/zeitonline/celery_longterm_scheduler',
    description="Schedules celery tasks to run in the potentially far future",
    long_description='\n\n'.join(
        open(x).read() for x in ['README.rst', 'CHANGES.txt']),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    zip_safe=False,
    license='BSD',
    install_requires=[
        'celery>=5.0.0.dev0',
        'click',
        'pendulum',
        'redis>=3.0',
        'setuptools',
    ],
    extras_require={'test': [
        'pytest',
        'testing.redis',
    ]},
    entry_points={
        'celery.commands': [
            'longterm_scheduler = celery_longterm_scheduler.scheduler:main',
        ]
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],

)
