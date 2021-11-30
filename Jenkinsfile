pipeline {
	 agent { docker { image 'python:3.7.1' } }
	
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
