Method Description:
- Library used:
    - pandas == 0.25.2
    - pyspark == 2.3.3
    - scikit-learn == 0.22
    - vaderSentiment == 3.2.1
- Utilizing the data from business.json, tip.json, and user.json. Even though it makes sense to utilize data from
review_train.json, since it will not contain the data of the testing set, I don't think it's going to have any
effect on the accuracy
    - Features extracted from business.json: business_id, latitude, longitude, review_count, stars
    - Features extracted from tip.json: user_id, business_id, text
    - Features extracted from user.json: user_id, review_count, average_stars, useful, funny, cool, fans
- Joining the data from train set and test set with the extracted data
- Calculating sentimental value of the user towards the business
- Using scikit-learn LinearRegression with an addition of PolynomialFeatures to perform a Non-linear Regression


Error Distribution:
- \>=0 and \<1: 101748
- \>=1 and \<2: 33377
- \>=2 and \<3: 6164
- \>=3 and \<4: 755
- \>=4: 0

RMSE: 0.980480954974841

Execution Time: 268.69662857055664 seconds
