import subprocess as sub

args = ['java', '-Dfile.encoding=UTF-8', 'no.priv.garshol.duke.Duke',
                '--linkfile=linkfile.txt']

run = sub.Popen(args, stderr=sub.PIPE, cwd=tmpdir, stdout=stdout,
                            universal_newlines=True)