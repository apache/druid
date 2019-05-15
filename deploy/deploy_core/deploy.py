
import sys
import os
current_path = os.path.abspath(__file__)
father_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".")
print father_path
sys.path.append(father_path + '/../utils')
import deploy_utils


if __name__ == '__main__':

    confs = deploy_utils.get_conf(father_path + '/deploy.properties')
    jenkins_param = deploy_utils.parse_jenkins_param(sys.argv[1])
    print "jenkins params", jenkins_param
    bin_dir = father_path + '/../' + confs['bin_dir']
    deploy_utils.main(confs, jenkins_param, bin_dir)