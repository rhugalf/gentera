pipeline {
	 agent none
	
	 stages {		
    		stage('Installing packages') {
			agent {
                		docker {
                    			image 'python:2-alpine' 
                		}
            		}
            		steps {
               		   	sh 'python --version'                    		
         		}
     		}
	}
}  
