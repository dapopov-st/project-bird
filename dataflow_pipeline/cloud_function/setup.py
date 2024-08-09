import setuptools

setuptools.setup(
    name='beam_pipeline',
    version='0.0.1',
    description='Project bird recent observations pipeline.',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'functions-framework',
        'build',
        "google-apitools==0.5.31",
        "google-cloud-core==2.4.1",
        "google-cloud-storage==2.18.0",
        "apache-beam==2.57.0",
    ],
 )