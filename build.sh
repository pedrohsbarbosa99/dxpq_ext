rm -rf build/ & \
rm -rf dist/ & \
rm -rf dxpq_ext.egg-inf & \
rm dxpq_ext.cpython-313-x86_64-linux-gnu.so & \
python setup.py clean --all && \
python setup.py build && \
pip install -e .
