from setuptools import setup, find_packages


setup(
    name='celery_longterm_scheduler',
    version='1.0.1',
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
        'celery',
        'pendulum',
        'redis',
        'setuptools',
    ],
    extras_require={'test': [
        'mock',
        'pytest',
        'testing.redis',
    ]},
    entry_points={
        'celery.commands': [
            'longterm_scheduler = celery_longterm_scheduler.scheduler:Command',
        ]
    }
)
