pipeline {
	 agent { docker { image 'python:3.5.1' } }
	 stages {		
    		stage('Installing packages') {
            		steps {
                		script {
                    			sh 'python --version'
                		}
         		}
     		}
	}
}  
